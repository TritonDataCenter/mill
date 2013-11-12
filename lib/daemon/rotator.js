/**
 * Copyright (c) 2013 Joyent Inc. All rights reserved.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_manta = require('manta');
var mod_once = require('once');
var mod_path = require('path');
var mod_vasync = require('vasync');
var mod_uuid = require('node-uuid');

/**
 * The mill manta uploader.
 */
function Rotator(opts, cb) {
    mod_assert.object(opts, 'opts');
    mod_assert.func(opts.cb, 'opts.cb');
    mod_assert.object(opts.manta, 'opts.manta');
    mod_assert.string(opts.manta.key, 'opts.manta.key');
    mod_assert.string(opts.manta.keyId, 'opts.manta.keyId');
    mod_assert.string(opts.manta.user, 'opts.manta.user');
    mod_assert.string(opts.manta.url, 'opts.manta.url');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.string(opts.instance, 'opts.instance');
    mod_assert.string(opts.service, 'opts.service');
    mod_assert.number(opts.interval, 'opts.interval');
    mod_assert.string(opts.location, 'opts.location');

    log.info({opts: opts}, 'Uplaoder:new: entering');

    this._mantaClient = mod_manta.createClient({
        sign: manta.privateKeySigner({
            key: fs.readFileSync(opts.manta.key),
            keyId: opts.manta.keyId,
            user: opts.manta.user
        }),
        user: opts.manta.user
        url: opts.manta.url
    });

    this._log = opts.log; /* bunyan logger */
    this._interval = opts.interval; /* upload interval in ms */
    this._service = opts.service; /* service name */
    this._instance = opts.instance; /* service instance */
    this._location = mod_path.normalize(opts.location); /* log on disk */
    this._locationDir = mod_path.dirname(self._location); /* log dir on disk */

    setInterval(moveTrunc, self._interval, self);

    //TODO: Should we respect hourly boundaries?

    return cb();
};
module.exports = Rotator;

/**
 * Copy and zero out the current log file.
 */
var moveTrunc = function moveTrunc(self, cb) {
    var log = self.log;
    log.info({location: location}, 'Rotator.moveTrunc: entering');
    cb = once(cb);

    function copyFile(source, target, cb) {
        var cbCalled = false;

        var rd = fs.createReadStream(source);
        rd.on("error", function(err) {
            done(err);
        });
        var wr = fs.createWriteStream(target);
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

    var copiedLog = self._locationDir + '/' + mod_uuid.v4();
    var rotatedLog;
    vasync.pipeline({funcs: [
        function _stat(_, _cb) {
            fs.stat(self._location, function (err, stats) {
                if (err) {
                    err = new verror.VError(err, 'unable to stat logfile');
                    return _cb(err);
                }

                // bail if size is 0
                if (stats.size === 0) {
                    log.info({location: self._location},
                             'Rotator.moveTrunc: not rotating and exiting, ' +
                             'log size 0');
                    return cb();
                }

                return _cb();
            });
        },
        function _copy(_, _cb) {
            copyFile(self._location, copiedLog, function (err) {
                if (err) {
                    err = new verror.VError(err, 'unable to copy log');
                    mod_fs.unlink(copiedLog, function(err) {
                        if (err) {
                            err = new verror.VError(err,
                                    'unable to delete copied log');
                        }
                        return _cb(err);
                    });
                }

                // cache the current time right after the copy so we can be as
                // exact as possible. TODO; actually parse the time from the log
                rotatedLog = self._location + '-' + new Date().getTime();
                return _cb();
            });
        },
        function _truncate(_, _cb) {
            mod_fs.truncate(self._location, 0, function (err) {
                if (err) {
                    err = new verror.VError(err, 'unable to truncate log');
                }

                return _cb();
            });
        },
        function _renameFile(_, _cb) {
            mod_fs.rename(copiedLog, rotatedLog, function (err) {
                if (err) {
                    err = new verror.VError(err, 'unable to rename copied log');
                }

                return _cb(err);
            });
        }
    ], arg:{}}, function (err) {
        log.info({
            copiedLog: copiedLog,
            rotatedLog: rotatedLog,
            log: self._location,
            err: err
        }, 'Rotator.moveTrunc: exiting');
    });
};

/**
 * Checks whether the log file exists, if the logfile DNE, watch and return
 * when the file is created.
 */
var checkExists = function checkExists(self, cb) {
    var log = self.log;
    log.info({location: location}, 'Rotator.checkExists: entering');
    cb = once(cb);

    vasync.pipeline({funcs: [
        function _statFile(_, _cb) {
            _cb = once(_cb);
            mod_fs.stat(self._location, function (err, stats) {
                if (err) {
                    err = new verror.VError(err);
                    return _cb(err);
                }

                if (stats.isDirectory) {
                    return _cb(new verror.VError('file: ' + self._location +
                                                 ' is a directory.'));
                }

                if (stats.isFile()) {
                    log.info({location: location}, 'Rotator.checkExists: ' +
                             'log file exists, exiting');
                    return cb();
                } else {
                    log.info({location: location}, 'Uplaoder.checkExists: ' +
                             'log file DNE, watching file');
                    return _cb();
                }
            });
        },
        function _watch(_, _cb) {

        }
    ], arg: {}}, function (err) {
        if (err) {
            err = new verror.VError(err, 'Error while checking log file');
        }
        return cb(err);
    });
};
