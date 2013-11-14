/**
 * Copyright (c) 2013 Joyent Inc. All rights reserved.
 */

var p = console.log;
var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_mkdirp = require('mkdirp');
var mod_mput = require('../common').mput;
var mod_once = require('once');
var mod_path = require('path');
var mod_sprintf = require('util').format;
var mod_PassThrough = require('stream').PassThrough;
var mod_Readable = require('stream').Readable;
var mod_vasync = require('vasync');
var mod_uuid = require('node-uuid');
var mod_zeroPad = require('../common').zeroPad;

var mod_errors = require('../errors'),
    InternalError = mod_errors.InternalError;

var VERSION = require('../../package.json').version;

/**
 * The mill manta uploader.
 */
function Rotator(opts) {
    var self = this;

    mod_assert.object(opts, 'opts');
    mod_assert.object(opts.mantaClient, 'opts.mantaClient');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.dataDir, 'opts.dataDir');
    mod_assert.string(opts.rotateDir, 'opts.rotateDir');
    mod_assert.object(opts.jobConfig, 'opts.jobConfig');
    mod_assert.object(opts.source, 'opts.source');
    mod_assert.string(opts.source.instance, 'opts.source.instance');
    mod_assert.string(opts.source.service, 'opts.source.service');
    mod_assert.number(opts.source.interval, 'opts.source.interval');
    mod_assert.string(opts.source.location, 'opts.source.location');
    mod_assert.string(opts.source.logType, 'opts.source.logType');

    var source = opts.source;
    opts.log.info({source: source}, 'Rotator: new');

    this._log = opts.log; /* bunyan logger */
    this._dataDir = opts.dataDir; /* base mill dir in Manta */
    this._mantaClient = opts.mantaClient;
    this._jobConfig = opts.jobConfig;
    this._rotateDir = opts.rotateDir; /* dir logs are rotated to */
    this._interval = source.interval; /* upload interval in ms */
    this._service = source.service; /* service name */
    this._instance = source.instance; /* service instance */
    this._location = mod_path.normalize(source.location); /* log on disk */
    this._locationDir = mod_path.dirname(self._location); /* log dir on disk */
    this._logType = source.logType; /* log type */
    this._header = opts.source; /* the header for the rotated logs */

    self._header.version = VERSION;
    self._header = JSON.stringify(self._header);

    this._isUpload = false; /* lock to ensure only 1 upload runs at a time */
};

Rotator.prototype.start = function start(cb) {
    var self = this;
    mod_mkdirp(self._rotateDir, 0744, function (err) {
        if (err) {
            return cb(new InternalError('unable to create log rotation dir'));
        }
        // always try and rotate when we first start
        moveTrunc(self, function(){
            //TODO: Should we respect hourly boundaries? We presently don't.
            setInterval(moveTrunc, self._interval, self, function(){});
        });

        return cb();
    });
};

/**
 * Copy and zero out the current log file.
 */
function moveTrunc(self, cb) {
    var log = self._log;
    log.info({
        location: self._location,
        rotateDir: self._rotateDir
    }, 'Rotator.moveTrunc: entering');
    cb = mod_once(cb);

    function copyFile(source, target, cb) {
        var cbCalled = false;

        var rd = mod_fs.createReadStream(source);
        rd.on("error", function(err) {
            done(err);
        });
        var wr = mod_fs.createWriteStream(target);
        wr.on("error", function(err) {
            done(err);
        });
        wr.on("close", function(ex) {
            done();
        });
        rd.pipe(wr);

        function done(err) {
            if (!cbCalled) {
                cb(err);
                cbCalled = true;
            }
        }
    }

    /*
     * TODO: make the copy atomic using this:
     * https://github.com/joyent/illumos-joyent/commit/c198c8b8de969665945a610aa6284cb8f82ed567
     * currently this will result in the loss of the last few chunks of the log
     * file between the copy and truncate
     */

    var rotatedLog = getRotatedPath(self);
    mod_vasync.pipeline({funcs: [
        function _stat(_, _cb) {
            mod_fs.stat(self._location, function (err, stats) {
                if (err) {
                    err = new InternalError(err, 'unable to stat logfile');
                    return _cb(err);
                }

                // bail if size is 0
                if (stats.size === 0) {
                    log.info({location: self._location},
                             'Rotator.moveTrunc: not rotating and exiting, ' +
                             'log size 0');
                    // but we kick off an upload in case there are outstanding uploads
                    upload(self, function(){});
                    return cb();
                }

                return _cb();
            });
        }, function _copy(_, _cb) {
            _cb = mod_once(_cb);
            copyFile(self._location, rotatedLog, function (err) {
                if (err) {
                    err = new InternalError(err, 'unable to copy log');
                    mod_fs.unlink(rotatedLog, function(err) {
                        if (err) {
                            err = new InternalError(err,
                                    'unable to delete copied log');
                        }
                        return _cb(err);
                    });
                }

                return _cb();
            });
        },
        function _truncate(_, _cb) {
            mod_fs.truncate(self._location, 0, function (err) {
                if (err) {
                    err = new InternalError(err, 'unable to truncate log');
                }

                return _cb(err);
            });
        },
        function _upload(_, _cb) {
            upload(self, function (err) {
                if (err) {
                    err = new InternalError(err, 'unable to upload logs');
                }
                return _cb(err);
            });
        }
    ], arg:{}}, function (err) {
        log.info({
            rotatedLog: rotatedLog,
            log: self._location,
            err: err
        }, 'Rotator.moveTrunc: exiting');

        return cb(err);
    });
};

function getRotatedPath(self) {
    return mod_sprintf('%s/%s:%s:%d.%s.log', self._rotateDir, self._service,
                       self._instance, Date.now(), self._logType);
};

/**
 * @param {object} self
 * @param {string} fileName The rotated log file of the format
 * yunongs-special-service:foo:1384293965917.generic.log
 */
function getMantaPath(self, fileName) {
    mod_assert.object(self, 'self');
    mod_assert.string(fileName, 'fileName');

    try {
        var timeInMs = fileName.split(':')[2].split('.')[0];
    } catch (e) {
        self._log.error({filename: fileName}, 'malformed filename');
        return;
    }
    var time = new Date(parseInt(timeInMs, 10));
    console.log(time);
    console.log(timeInMs);
    var year = time.getUTCFullYear();
    var month = mod_zeroPad(time.getUTCMonth() + 1, 2);
    var date = mod_zeroPad(time.getUTCDate(), 2);
    var hour = mod_zeroPad(time.getUTCHours(), 2);

    // just 1384293965917.generic.log
    var file = fileName.split(':')[2];

    // /$user/stor/mill/logs/$service/$year/$month/$day/$hour/$instance-$timestamp.$type.log
    var path = mod_sprintf('%s/logs/%s/%s/%s/%s/%s/%s-%s', self._dataDir,
                           self._service, year, month, date, hour,
                           self._instance, file);

    self._log.debug({mantaPath: path}, 'getMantaPath: exiting');
    return path;
};

function submitJob(self, keys, cb) {
    var log = self._log;
    var cb = mod_once(cb);
    var m = self._mantaClient;
    var jobName = self._jobConfig.name
    log.info({jobName: jobName, keys: keys},
        'Rotator.submitJob: entering');

    mod_vasync.pipeline({funcs: [
        function _getJob(_, _cb) {
            m.listJobs({name: jobName, state: 'running'}, function (err, res) {
                if (err) {
                    return _cb(new InternalError('unable to get job'));
                }

                _.jobs = [];
                res.on('job', function (j) {
                    _.jobs.push(j);
                });

                res.once('error', function (err) {
                    return _cb(new InternalError(err, 'unable to get job'));
                });

                res.once('end', function() {
                    if (_.jobs.length === 0) {
                        return _cb();
                    }
                    _.jobId = _.jobs[_.jobs.length - 1].name;
                    return _cb();
                });
            });
        },
        function _createNewJob(_, _cb) {
            if (_.jobs.length > 0) {
                return _cb();
            }

            m.createJob(self._jobConfig, {name: jobName},
                        function (err, jobId) {
                if (err) {
                    return _cb(new InternalError(err, 'unable to create job'));
                }

                _.jobId = jobId;
                return _cb();
            });
        },
        function _submitKeys(_, _cb) {
            log.info({keys: keys, jobId: _.jobId}, 'submitKeys')
            m.addJobKey(_.jobId, keys, function (err) {
                if (err) {
                    err = new InternalError(err, 'unable to add keys to job');
                }

                return _cb(err);
            });
        }
    ], arg:{}}, function (err) {
        log.info({err: err}, 'Rotator.submitJob: exiting');
        return cb(err);
    });
}

function upload(self, cb) {
    var log = self._log;
    var cb = mod_once(cb);
    log.info({lock: self._isUpload}, 'Rotator.upload: entering');

    if (self._isUpload) {
        return callback();
    }

    self._isUpload = true;

    mod_vasync.pipeline({funcs: [
        function _listDir(_, _cb) {
            mod_fs.readdir(self._rotateDir, function (err, files) {
                if (err) {
                    err = new InternalError(err,
                        'unable to list rotate log dir');
                    return _cb(err);
                }

                if (files.length === 0) {
                    log.info('Rotator.upload: exit, no files to upload');
                    self._isUpload = false;
                    return cb();
                }
                _.files = files;
                return _cb();
            });
        },
        /* Get smart about uploading, check for the file in Manta first before
         * uploading it. It's possible the last run failed somewhere in or
         * after upload. In which case we probably didn't rm all of the files
         * from disk.
         */
        function _upload(_, _cb) {
            _cb = mod_once(_cb);
            var barrier = mod_vasync.barrier();
            _.completedFiles = [];

            barrier.on('drain', function () {
                log.info({files: _.files}, 'all files uploaded');
                return _cb();
            });

            _.keys = [];


            var i = 0;
            _.files.forEach(function (file) {
                barrier.start('mput ' + file);
                // if the path isn't valid, we bail on uploading.
                var path = getMantaPath(self, file);
                if (!path) {
                    barrier.done('mput ' + file);
                    return;
                }

                var fullFilePath = self._rotateDir + '/' + file;
                log.info({path: path, file: fullFilePath},
                         'Rotator.upload: uploading logs');
                // add the header
                var ps = new mod_PassThrough();
                ps.push(self._header);
                ps.push('\n');
                mod_fs.createReadStream(fullFilePath).pipe(ps);
                //TODO: make this configurable with retries
                self._mantaClient.put(path, ps, {mkdirs: true}, function (err) {
                    barrier.done('mput ' + file);
                    if (err) {
                        self._isUpload = false;
                        return cb(new InternalError(err,
                                                     'could not upload log'));
                    }
                    _.keys.push(path);
                    log.info({path: path, file: fullFilePath},
                             'Rotator.upload: finished uploading log');
                    _.completedFiles.push(fullFilePath);

                });
            });
        },
        function _submitJob(_, _cb) {
            submitJob(self, _.keys, function (err) {
                if (err) {
                    err = new InternalError(err, 'unable to submit job');
                }

                return _cb(err);
            });
        },
        function _rm(_, _cb) {
            var barrier = mod_vasync.barrier();
            barrier.on('drain', function () {
                log.info({files: _.completedFiles}, 'all files deleted');
                return _cb();
            });

            _.completedFiles.forEach(function (file) {
                barrier.start('unlink ' + file);
                mod_fs.unlink(file, function (err) {
                    barrier.done('unlink ' + file);
                    // ignore delete errors -- they'll get uploaded on the next
                    // pass
                });
            });
        }
    ], arg:{}}, function (err, results) {
        if (err) {
            err = new InternalError(err, 'unable to upload all logs');
            log.error({err: err});
        }

        log.info('Rotator.upload: exiting');
        self._isUpload = false;
        return cb(err);
    });
};


/// Exports

module.exports = {
    createRotator: function createRotator(opts) {
        return new Rotator(opts);
    }
};



/// Main

if (require.main === module) {
    var mod_manta = require('manta');

    var log = mod_bunyan.createLogger({name: "rotator-test"});
    var opts = {
        keyId: '9d:1c:f4:69:66:cb:bf:1a:40:b5:d2:c2:6a:0a:eb:2d',
        user: 'yunong',
        url: 'https://us-east.manta.joyent.com',
        key: '/Users/yunong/.ssh/id_rsa'
    };
    var mantaClient = mod_manta.createClient({
        sign: mod_manta.cliSigner({
            keyId: opts.manta.keyId,
            user: opts.manta.user
        }),
        user: opts.manta.user,
        url: opts.manta.url,
        log: log
    });

    new Rotator({
        mantaClient: mantaClient,
        log: log,
        dataDir: '/yunong/stor/mill',
        source: {
            instance: 'foo',
            service: 'yunongs-special-service',
            interval: 20 * 1000,
            location: '/tmp/yunonglog',
            rotateDir: '/tmp/mill/rotatedLogs',
            logType: 'generic'
        },
    }, function (err)  {
        console.log('rotator started');
    });
}

// vim: set softtabstop=4 shiftwidth=4:
