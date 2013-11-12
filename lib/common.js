#!/usr/bin/env node
/**
 * Copyright (c) 2013 Joyent Inc. All rights reserved.
 */

var p = console.log;
var assert = require('assert-plus');
var fs = require('fs');


function loadConfigSync(configPath) {
    var config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    if (!config.account) {
        config.account = process.env.MANTA_USER;
    }
    if (!config.url) {
        config.url = process.env.MANTA_URL;
    }
    if (!config.keyId) {
        config.keyId = process.env.MANTA_KEY_ID;
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

function zeroPad(n, width) {
    var s = String(n);
    while (s.length < width) {
        s = '0' + s;
    }
    return s;
}

function runJob(options, callback) {
    assert.object(options, 'options');
    assert.object(options.client, 'options.client');
    assert.object(options.jobSpec, 'options.jobSpec');
    assert.optionalString(options.name, 'options.name');
    assert.arrayOfString(options.inputs, 'options.inputs');
    assert.optionalBool(options.gatherOutputs, 'options.gatherOutputs');
    assert.optionalObject(options.streamOutputs, 'options.streamOutputs');
    assert.func(callback, 'callback');
    var client = options.client;

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
                e('  * started job %s (%s)', jobId, options.name);
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
                        next(new Error(format('Job %s (%s) cancelled', jobId, options.name)));
                    } else if (job.stats.errors) {
                        next(new Error(format('Job %s (%s) had errors: %j', jobId, options.name, job)));
                    } else {
                        e('  * job %s (%s) succeeded', jobId, options.name);
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



//---- exports

module.exports = {
    loadConfigSync: loadConfigSync,
    zeroPad: zeroPad,
    runJob: runJob
};
// vim: set softtabstop=4 shiftwidth=4:
