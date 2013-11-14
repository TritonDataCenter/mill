#!/usr/bin/env node
/**
 * Back fill log files into mill.
 */

var p = console.log;
var mod_bunyan = require('bunyan');
var mod_exec = require('child_process').exec;
var mod_manta = require('manta');
var mod_sprintf = require('util').format;
var mod_vasync = require('vasync');
var mod_lstream = require('lstream');
var mod_TStream = require('stream').Transform;
var mod_WStream = require('stream').Writable;
var mod_zeroPad = require('../lib/common').zeroPad;
var mod_PStream = require('stream').PassThrough;

// ghetto hack the interval to 5 mins for now
var interval = process.env.MILL_INTERVAL || 3 * 1000;
var dataDir = process.env.MILL_DATA_DIR;
var service = process.env.MILL_SERVICE;
var instance = process.env.MILL_INSTANCE;
var type = process.env.MILL_LOG_TYPE || 'generic';

var s = process.stdin.pipe(new mod_lstream());

var log = mod_bunyan.createLogger({
    name: 'milld-backfill',
    serializers: mod_bunyan.stdSerializers,
    stream: process.stderr,
    level: 'info'
});

var prevInterval;
var mput;
var __lines = 0;
var ended;
var manta = mod_manta.createBinClient({log: log});
var ps;

var barrier = mod_vasync.barrier();
barrier.on('drain', function () {
    console.log('finished uploading all files');
    process.exit();
});

s.on('readable', function () {
    var line = s.read();
    if (!line) {
        return
    };
    do {
        var ts = parseInt(line.substr(0, 14), 10);

        if (!prevInterval) {
            prevInterval = ts;
            s.pause();
            ps = new mod_PStream();
            ps.push(line + '\n');
            var path = getMantaPath(ts);
            p('mputting ' + path);
            barrier.start('mput ' + path);
            manta.put(path, ps, {mkdirs: true}, function (err) {
                p('returned from put' + path);
                barrier.done('mput ' + path);
                if (err) {
                    console.error(err, 'unable to upload to manta');
                    log.fatal({err: err});
                    process.exit(1);
                }
            });
            s.resume();
        } else {
            var diff = ts - prevInterval;
            if (diff >= interval) {
                prevInterval = ts;
                s.pause();
                ps.push(null);
                ps = new mod_PStream();
                ps.push(line + '\n');
                var path = getMantaPath(ts);
                p('mputting ' + path);
                barrier.start('mput ' + path);
                manta.put(path, ps, {mkdirs: true}, function (err) {
                    p('returned from put' + path);
                    barrier.done('mput ' + path);
                    if (err) {
                        console.error(err, 'unable to upload to manta');
                        log.fatal({err: err});
                        process.exit(1);
                    }
                });
                s.resume();
            }
        }
        ps.push(line + '\n');
        line = s.read();
    } while (line);
});

s.on('error', function (err) {
    console.error(err, 'backfill failed');
    process.exit(1);
});

s.on('end', function () {
    p('finished reading all lines');
    ps.push(null);
    ended = true;
});


function getMantaPath(timeInMs) {
    var time = new Date(parseInt(timeInMs, 10));
    var year = time.getUTCFullYear();
    var month = mod_zeroPad(time.getUTCMonth() + 1, 2);
    var date = mod_zeroPad(time.getUTCDate(), 2);
    var hour = mod_zeroPad(time.getUTCHours(), 2);

    // /$user/stor/mill/logs/$service/$year/$month/$day/$hour/$instance-$timestamp.$type.log
    var path = mod_sprintf('%s/logs/%s/%s/%s/%s/%s/%s-%s.%s.log', dataDir,
                           service, year, month, date, hour,
                           instance, timeInMs, type);

    return path;
};

