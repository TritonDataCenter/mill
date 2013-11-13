#!/usr/bin/env node
/*
 * A long running bunyan app that just logs stuff somewhat randomly.
 *
 * Usage:
 *      ./a-logging-app.js NAME
 *
 * Logs to '/var/tmp/NAME.log'.
 */

var p =  console.log;
var bunyan = require('bunyan');

if (process.argv.length !== 3) {
    throw new Error('incorrect number of args');
}
var name = process.argv[2];
var log = bunyan.createLogger({
    name: name,
    streams: [{
        path: '/var/tmp/' + name + '.log'
    }],
    level: 'debug'
});

function randRange(lo, hi) {
    return (Math.random() * (hi - lo)) + lo;
}

function randSelect(items) {
    var i = Math.floor(randRange(0, items.length));
    return items[i];
}

var xyWords = ('skatoxyl splenopexy stannoxyl stomoxys sulfoxylate ' +
    'sulfoxylic sulphoxylate sulphoxylic sulphoxyphosphate superoxygenate ' +
    'superoxygenation taxy tetracarboxylate tetracarboxylic tetrahydroxy ' +
    'Toxylon tricarboxylic trihydroxy trimethoxy trinitroxylene ' +
    'trinitroxylol trioxymethylene Trixy typhlopexy ultraorthodoxy unfoxy ' +
    'unorthodoxy unoxygenated unoxygenized').split(/\s+/g);

function logAndAgain() {
    var lvl = randSelect([
        'trace',
        'debug',
        'info',
        'warn',
        'error',
        'fatal'
    ]);
    log[lvl]({xy: randSelect(xyWords)}, 'do a ' + randSelect(xyWords) + ' thing');
    setTimeout(logAndAgain, randRange(500, 10000));
}

setTimeout(logAndAgain, randRange(500, 3000));
