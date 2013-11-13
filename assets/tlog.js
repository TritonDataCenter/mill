#!/usr/bin/env node
/**
 * Transform a raw .log file uploaded by milld to a .tlog file.
 *
 * The .log file's first line should be a JSON object header
 * with the log file "type" and optional additional info needed to
 * reliably parse it. For example, an "nginx"-type log generally
 * needs the nginx config's "log_format" line -- which differ in
 * order widely apparently.
 *
 * The .log file's "type" (without supporting header info) is
 * also in the file extension: "$instance-$timestamp.$type.$log".
 *
 * The .tlog format is:
 *
 *      $timestamp,$original_log_line
 *
 * where "$timestamp" is a number of milliseconds since the epoch.
 *
 * TODO:
 * - "multi-line log records"  An idea is to prefix each of these
 *   with "$timestamp," using the previous successfully parsed
 *   log record's timestamp
 * - If the log *starts* with lines that can't be parsed we might
 *   be (a) unlucky to start inside a "multi-line log record", or
 *   (b) in a log file format that doesn't have timestamps at all.
 *   Soln: Buffer up to ~1MiB of lines trying to find a line with
 *   with a timestamp and use that... else just use the $timestamp
 *   from the *file*.
 */

var p = console.log;
var stream = require('stream');
var Transform = stream.Transform;
var assert = require('assert');
var util = require('util');

//var debug = p;
var debug = function() {};

if (!Transform)
  throw new Error('Sorry, your node is too crusty, get a new one')


function Tlog(options) {
    this._prevDate = 0;
    this._buffer = '';
    Transform.call(this, options);
}
util.inherits(Tlog, Transform);
module.exports = Tlog;

Tlog.prototype._transform = function(chunk, encoding, cb) {
    debug('_transform:', chunk)
    if (typeof chunk !== 'string') {
        chunk = chunk.toString('utf8');
    } else if (encoding !== 'utf8') {
        chunk = new Buffer(chunk, encoding).toString('utf8');
    }
    var lines = (this._buffer + chunk).split('\n');
    this._buffer = lines.pop();
    lines.forEach(this._parseLine, this)
    cb();
};

var BUNYAN_TIME_RE = /^\{.*,"time":"([0-9T:Z.-]+)".*\}$/;
var CLF_TIME_RE = /([0-9]{1,2})\/([A-Za-z]{3})\/([0-9]{4}):([0-9]{2}:[0-9]{2}:[0-9]{2})( ([+-][0-9]{4}|Z))/
var ISO_TIME_RE = /([0-9]{4}-[0-9]{2}-[0-9]{2})(?:T([0-9]{2}:[0-9]{2})(:[0-9]{2}(\.[0-9]+)?)?)?(Z|[+-][0-9]{2}:[0-9]{2}|[+-][0-9]{4})?/

Tlog.prototype._parseLine = function(line, index, lines) {
    debug('_parseLine %j index=%j', line, index);
    var bun, iso, clf, date;

    // bunyan is a simpler regexp, so try that first.
    var bun = BUNYAN_TIME_RE.exec(line);
    if (bun)
      date = new Date(bun[1]).getTime();

    if (!date) {
      iso = ISO_TIME_RE.exec(line);
      if (iso)
        date = new Date(iso[0]).getTime();
    }

    if (!date) {
      var c = CLF_TIME_RE.exec(line);
      if (c) {
        clf = c[1] + ' ' + c[2] + ' ' + c[3] + ' ' + c[4] + ' ' + c[6];
        date = new Date(clf).getTime();
      }
    }

    if (!date) {
      debug('_parseLine failed. line:%j %j', index, line);
      date = this._prevDate;
    } else {
      this._prevDate = date;
    }
    this.push(date + ',' + line + '\n', 'utf8');
};

if (require.main === module) {
    var tlog = new Tlog();
    process.stdin.pipe(tlog).pipe(process.stdout);
}

