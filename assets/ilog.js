var stream = require('stream');
var Transform = stream.Transform;
var assert = require('assert');
var util = require('util');

var debug = util.debuglog && util.debuglog('ilog');
if (!debug)
  debug = function() {};

// debug = console.error;

if (!Transform)
  throw new Error('Sorry, your node is too crusty, get a new one')


module.exports = Ilog;

util.inherits(Ilog, Transform);

function Ilog(options) {
  this._head = false;
  this._type = null;
  this._format = null;
  this._buffer = '';
  Transform.call(this, options);
}

Ilog.prototype._transform = function(chunk, encoding, cb) {
  if (typeof chunk !== 'string')
    chunk = chunk.toString('utf8');
  else if (encoding !== 'utf8')
    chunk = new Buffer(chunk, encoding).toString('utf8');

  var lines = (this._buffer + chunk).split('\n');
  this._buffer = lines.pop();

  // parse all the lines!
  lines.forEach(this._parseLine, this)
  cb();
};

Ilog.prototype._parseLine = function(line, index, lines) {
  if (!this._head) {
    this._head = JSON.parse(line);
    debug('head', this._head);
    this._type = this._head.source.type;
    if (this._type === 'nginx')
      this._format = parseNginxFormat(this._head.source.format);
    this.push(line + '\n', 'utf8');
  } else if (this._type === 'nginx')
    this._parseNginxLine(line, index, lines);
  else
    this._parseBunyanLine(line, index, lines);
};


Ilog.prototype._parseBunyanLine = function (line, index, lines) {
  this.push(line + '\n', 'utf8');
};


Ilog.prototype._parseNginxLine = function(line, index, lines) {
  var ts = parseInt(line, 10);
  line = line.substr(('' + ts).length + 1);
  var date = new Date(ts);

  var obj = {};
  var l = line;
  var invalid = false;
  this._format.forEach(function(word) {
    if (invalid) return;

    debug('>>%s<< %j', word, l);
    // chop off a chunk of the line, and update the object.
    if (!word.match(/^\$.+?\b/)) {
      // just some literal text.  walk over it.
      if (l.slice(0, word.length) !== word) {
        // invalid log line, skip and warn
        console.error('Invalid log line: %j', line);
        invalid = true;
        return;
      }
      l = l.slice(word.length);
      return;
    }

    var re = null;
    var number = false;
    var key = word.slice(1);
    switch (word) {
      case '$body_bytes_sent':
      case '$bytes_sent':
      case '$connection':
      case '$request_length':
      case '$status':
      case '$msec':
      case '$status':
        number = true;
        re = /\d+/;
        break;

      case '$pipe':
        re = /./;
        break

      case '$remote_user':
        re = /[^ ]+/;
        break;

      case '$remote_addr':
      case '$http_x_forwarded_for':
        re = /[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|-/;
        break;

      case '$request':
        re = /[A-Z]+ [^ ]+ HTTP\/(1\.0|1\.1|0\.9)/;
        break;

      case '$time_local':
        re = clfRe;
        dateParse = parseClf;
        break;

      case '$time_iso8601':
        re = isoRe;
        dateParse = parseIso;
        break;

      case '$http_referrer':
      case '$http_user_agent':
      default:
        re = /[^"]+/;
        break;
    }

    obj[key] = chomp(l, re);
    if (obj[key]) {
      l = l.slice(obj[key].length);

      if (number)
        obj[key] = +obj[key];
    }
  });

  delete obj.time_local;
  delete obj._time_iso8601;
  obj.time = date.toISOString();

  this.push(ts + ',' + JSON.stringify(obj) + '\n', 'utf8');
};

function parseClf(c) {
  c = clfRe.exec(c);
  return new Date(c[1] + ' ' + c[2] + ' ' + c[3] + ' ' + c[4] + ' ' + c[6]);
}

function parseIso(c) {
  return new Date(c);
}

var clfRe = /([0-9]{1,2})\/([A-Za-z]{3})\/([0-9]{4}):([0-9]{2}:[0-9]{2}:[0-9]{2})( ([+-][0-9]{4}|Z))/
var isoRe = /([0-9]{4}-[0-9]{2}-[0-9]{2})(?:T([0-9]{2}:[0-9]{2})(:[0-9]{2}(\.[0-9]+)?)?)?(Z|[+-][0-9]{2}:[0-9]{2}|[+-][0-9]{4})?/

var wordRe = /([^\\]|^)\$(.+?)\b/g;
var wordRepl = '$1\0$$$2\0';
function parseNginxFormat(logFormat) {
  return logFormat.replace(wordRe, wordRepl).split('\0').filter(function(w) {
    return !!w;
  });
}

function chomp(line, re) {
  var cut = re.exec(line);
  if (cut) cut = cut[0];
  return cut;
}

if (require.main === module) {
    var ilog = new Ilog();
    process.stdin.pipe(ilog).pipe(process.stdout);
}
