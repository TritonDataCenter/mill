#!/usr/bin/env node
/**
 * Copyright (c) 2013 Joyent Inc. All rights reserved.
 */

var p = console.log;
var assert = require('assert-plus');
var async = require('async');
var backoff = require('backoff');
var fs = require('fs');
var once = require('once');
var util = require('util'),
    format = util.format;
var verror = require('verror');

var errors = require('./errors'),
    InternalError = errors.InternalError;


function loadConfigSync(configPath) {
    var config = {};
    if (fs.existsSync(configPath)) {
        config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    }
    if (!config.account) {
        config.account = process.env.MANTA_USER;
    }
    if (!config.url) {
        config.url = process.env.MANTA_URL;
    }
    if (!config.keyId) {
        config.keyId = process.env.MANTA_KEY_ID;
    }
    if (!config.dataDir) {
        config.dataDir = process.env.MILL_DIR ||
            format('/%s/stor/mill', config.account);
    }
    if (!config.rotateDir) {
        config.rotateDir = '/var/tmp/mill';
    }
    checkConfig(config);
    return config;
}

function checkConfig(config) {
    if (!config.account) {
        throw new Error('config has no Manta "account"');
    }
    if (!config.url) {
        throw new Error('config has no Manta "url"');
    }
    if (!config.keyId) {
        throw new Error('config has no Manta "keyId"');
    }
}


function objCopy(obj) {
    var copy = {};
    Object.keys(obj).forEach(function (k) {
        copy[k] = obj[k];
    });
    return copy;
}


function zeroPad(n, width) {
    var s = String(n);
    while (s.length < width) {
        s = '0' + s;
    }
    return s;
}

function runJob(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.client, 'options.client');
    assert.object(options.jobSpec, 'options.jobSpec');
    assert.arrayOfString(options.inputs, 'options.inputs');
    assert.optionalBool(options.gatherOutputs, 'options.gatherOutputs');
    assert.optionalObject(options.streamOutputs, 'options.streamOutputs');
    assert.func(callback, 'callback');
    var log = options.log;
    var client = options.client;

    var jobName = options.jobSpec.name;
    var jobId;
    var job;
    var outputs;
    async.series([
        function doCreate(next) {
            client.createJob(options.jobSpec, function (err, id) {
                if (err) {
                    return next(err);
                }
                jobId = id;
                log.info('started job %s (%s)', jobId, jobName);
                next();
            });
        },
        function addInputs(next) {
            client.addJobKey(jobId, options.inputs, function (err) {
                if (err) return next(err);
                client.endJob(jobId, next);
            });
        },
        function doWait(next) {
            // TODO: could add a progbar on number of keys processed
            setTimeout(poll, 1000);

            function poll() {
                client.job(jobId, function (err, job_) {
                    if (err) return next(err);
                    job = job_;
                    if (job.state !== 'done') {
                        setTimeout(poll, 1000);
                    } else if (job.cancelled) {
                        next(new InternalError(format('Job %s (%s) cancelled', jobId, jobName)));
                    } else if (job.stats.errors) {
                        next(new InternalError(format('Job %s (%s) had errors: %j', jobId, jobName, job)));
                    } else {
                        log.info('job %s (%s) succeeded', jobId, jobName);
                        next();
                    }
                });
            }
        },
        function getOutputs(next) {
            if (!options.gatherOutputs) {
                return next();
            }
            client.jobOutput(jobId, function (err, out) {
                if (err) return next(err);
                var keys = [];
                out.on('key', function (key) { keys.push(key); });
                out.once('end', function () {
                    outputs = {};
                    async.each(
                        keys,
                        function getKey(key, nextKey) {
                            client.get(key, function (err, stream) {
                                var chunks = [];
                                stream.on('data', function (chunk) {
                                    chunks.push(chunk);
                                });
                                stream.once('end', function () {
                                    outputs[key] = chunks.join('');
                                    nextKey();
                                });
                            });
                        },
                        next
                    )
                });
            });
        },
        function streamOutputs(next) {
            if (!options.streamOutputs) {
                return next();
            }
            client.jobOutput(jobId, function (err, out) {
                if (err) return next(err);
                var keys = [];
                out.on('key', function (key) { keys.push(key); });
                out.once('end', function () {
                    outputs = {};
                    async.each(
                        keys,
                        function getKey(key, nextKey) {
                            client.get(key, function (err, stream) {
                                stream.pipe(options.streamOutputs);
                                stream.once('end', function () {
                                    nextKey();
                                });
                            });
                        },
                        next
                    )
                });
            });
        }
    ], function done(err) {
        callback(err, {job: job, outputs: outputs});
    });
}

/**
 * pretty much directly stolen from node-manta/bin/mput
 */
function mput(options, callback) {
    assert.string(options.path, 'options.path');
    assert.string(options.file, 'options.file');
    assert.object(options.client, 'options.client');
    assert.optionalString(options.md5, 'options.md5');
    assert.optionalObject(options.retry, 'options.retry');
    assert.optionalNumber(options.copies, 'options.copies');

    var log = options.log;
    var opts = {
        copies: options.copies,
        headers: options.headers,
        mkdirs: options.parents
    };
    options.retry = options.retry || {};
    var client = options.client;
    callback = once(callback);

    log.info({file: options.file}, 'mput:entering');
    function ifError(err) {
        if (!err) {
            return;
        }

        return callback(new verror.VError(err));
    }

    function put(stream, stats, cb) {
        log.warn('foo');
        client.info(options.path, function (info_err, info) {
            if (info_err && info_err.statusCode !== 404) {
                    if (cb) {
                        cb(info_err);
                    } else {
                        if (err) {
                            return callback(new verror.VError(err));
                        }
                    }
            } else if (info) {
                if (info.type === 'application/x-json-stream; type=directory') {
                    options.path += '/' + path.basename(options.file);
                }
            }

            client.put(options.path, stream, opts, function (err) {
                if (err) {
                    if (cb) {
                        cb(err);
                    } else {
                        if (err) {
                            return callback(new verror.VError(err));
                        }
                    }
                }
                client.close();
                if (cb)
                    cb();
            });
        });
    }

    fs.stat(options.file, function (err, stats) {
        if (err) {
            return callback(new verror.VError(err));
        }
        if (!stats.isFile()) {
            return callback(new verror.VError(options.file + ' is not a file'));
        }

        opts.size = stats.size;

        function write(_, cb) {
            cb = once(cb);
            var f_opts = {
                start: 0,
                end: stats.size - 1
            };
            var fstream = fs.createReadStream(options.file, f_opts);
            fstream.pause();
            fstream.on('open', function () {
                put(fstream, stats, cb);
            });
        }

        function md5() {
            var f_opts = {
                start: 0,
                end: stats.size - 1
            };
            var fstream = fs.createReadStream(options.file, f_opts);
            var hash = crypto.createHash('md5');

            fstream.on('data', hash.update.bind(hash));
            fstream.once('end', function () {
                opts.headers['Content-MD5'] = hash.digest('base64');
                upload();
            });
        }

        function upload() {
            var retry = backoff.call(write, null, ifError);
            retry.setStrategy(new backoff.ExponentialStrategy({
                initialDelay: options.retry.initialDelay || 1000,
                maxDelay: options.retry.maxDelay | 10000
            }));
            retry.failAfter(options.retry.number || 5);
            retry.on('backoff', function (num, delay, error) {
                // If we set a HTTP/1.1 Conditional PUT header and the
                // precondition was not met, then bail out without retrying:
                if (error && error.name === 'PreconditionFailedError') {
                    return callback(new verror.VError(error));
                }
                log.debug({
                    err: error,
                    num: num,
                    delay: delay
                }, 'request failed. %s', num === 3 ? 'fail' : 'retrying');
            });
            retry.start();
        }

        if (options.md5) {
            md5();
        } else {
            upload();
        }
    });
}



//---- exports

module.exports = {
    loadConfigSync: loadConfigSync,
    objCopy: objCopy,
    zeroPad: zeroPad,
    runJob: runJob,
    mput: mput,
};
// vim: set softtabstop=4 shiftwidth=4:
