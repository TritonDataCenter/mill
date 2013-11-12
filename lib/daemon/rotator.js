/**
 * Copyright (c) 2013 Joyent Inc. All rights reserved.
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_manta = require('manta');
var mod_mkdirp = require('mkdirp');
var mod_once = require('once');
var mod_path = require('path');
var mod_sprintf = require('util').format;
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_uuid = require('node-uuid');

/**
 * The mill manta uploader.
 */
function Rotator(opts, cb) {
    var self = this;

    mod_assert.object(opts, 'opts');
    mod_assert.optionalFunc(cb, 'cb');
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
    mod_assert.string(opts.rotateDir, 'opts.rotateDir');
    mod_assert.string(opts.logType, 'opts.logType');

    opts.log.info({opts: opts}, 'Uplaoder:new: entering');

    this._mantaClient = mod_manta.createClient({
        sign: mod_manta.privateKeySigner({
            key: opts.manta.key,
            keyId: opts.manta.keyId,
            user: opts.manta.user
        }),
        user: opts.manta.user,
        url: opts.manta.url
    });

    this._log = opts.log; /* bunyan logger */
    this._interval = opts.interval; /* upload interval in ms */
    this._service = opts.service; /* service name */
    this._instance = opts.instance; /* service instance */
    this._location = mod_path.normalize(opts.location); /* log on disk */
    this._locationDir = mod_path.dirname(self._location); /* log dir on disk */
    this._rotateDir = opts.rotateDir; /* dir logs are rotated to */
    this._logType = opts.logType; /* log type */

    this._isUpload = false; /* lock to ensure only 1 upload runs at a time */
    mod_mkdirp(self._rotateDir, 0744, function (err) {
        if (err) {
            return cb(new mod_verror.VError('unable to create log rotation dir'));
        }
        setInterval(moveTrunc, self._interval, self, function(){});

        //TODO: Should we respect hourly boundaries? We presently don't.

        return cb();
    });
};
module.exports = Rotator;

/**
 * Copy and zero out the current log file.
 */
var moveTrunc = function moveTrunc(self, cb) {
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
                    err = new mod_verror.VError(err, 'unable to stat logfile');
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
            copyFile(self._location, rotatedLog, function (err) {
                if (err) {
                    err = new mod_verror.VError(err, 'unable to copy log');
                    mod_fs.unlink(rotatedLog, function(err) {
                        if (err) {
                            err = new mod_verror.VError(err,
                                    'unable to delete copied log');
                        }
                        return _cb(err);
                    });
                }

                // cache the current time right after the copy so we can be as
                // exact as possible. TODO: actually parse the time from the log
                rotatedLog = self._location + '-' + new Date().getTime();
                return _cb();
            });
        },
        function _truncate(_, _cb) {
            mod_fs.truncate(self._location, 0, function (err) {
                if (err) {
                    err = new mod_verror.VError(err, 'unable to truncate log');
                }

                return _cb();
            });
        }
    ], arg:{}}, function (err) {
        log.info({
            rotatedLog: rotatedLog,
            log: self._location,
            err: err
        }, 'Rotator.moveTrunc: exiting');
    });
};

var getRotatedPath = function getRotatedPath(self) {
    return mod_sprintf('%s/%s:%s:%d.%s.log', self._rotateDir, self._service,
                       self._instance, new Date().getTime(), self._logType);
};

var upload = function upload(self, cb) {
    var log = self._log;
    cb = once(cb);
    log.info({lock: self._isUpload}, 'Rotator.upload: entering');

    if (self._isUpload) {
        return cb();
    }

    mod_vasync.pipeline({funcs: [
        function _listDir(_, _cb) {

        },
        function _upload(_, _cb) {

        }
    ], arg:{}}, function (err) {

    });
};

/// Main

new Rotator({
    manta: {
        keyId: '9d:1c:f4:69:66:cb:bf:1a:40:b5:d2:c2:6a:0a:eb:2d',
        user: 'yunong',
        url: 'https://us-east.manta.joyent.com',
        key: '/Users/yunong/.ssh/id_rsa'
    },
    log: mod_bunyan.createLogger({name: "rotator-test"}),
    instance: 'foo',
    service: 'yunongs-special-service',
    interval: 1 * 1000,
    location: '/tmp/yunonglog',
    rotateDir: '/tmp/mill/rotatedLogs',
    logType: 'generic'
}, function (err)  {
    console.log('rotator started' + err);
});
// vim: set softtabstop=4 shiftwidth=4:
