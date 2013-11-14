// pipe a ilog in, and a bunch of command line configs, and it'll
// output just the selected data

module.exports = Search;
var Transform = require('stream').Transform;
if (!Transform)
  throw new Error('your node is too crusty and old');
var util = require('util');
util.inherits(Search, Transform);

var debug = console.error;
var debug = function() {};

function Search(argv, options) {
  Transform.call(this, options);
  this._filters = [];
  this._select = [];
  this._nextIsOutput = false;
  this._buffer = '';
  this._output = 'tab';

  // this must be last.
  this._parseArgs(argv);
}

Search.prototype._parseArgs = function(argv) {
  argv.forEach(this._parseArg, this);
  debug('filters =', this._filters);
}

Search.prototype._parseArg = function(arg, i, argv) {
  var kv = arg.match(/^([a-zA-Z0-9_]+)([><]=*|=~|!=|=+)(.*)$/);
  debug('%j', arg, kv, this._filters);

  if (kv) {
    var key = kv[1];
    var eq = kv[2];
    var val = kv[3];
    switch (eq) {
      case '!=':
        this._filters.push(neqFilter(key, val));
        break;
      case '~=':
      case '=~':
        debug('push regexfilter');
        this._filters.push(regexFilter(key, val));
        break;
      case '=':
        this._filters.push(stringFilter(key, val));
        break;
      case '>=':
        this._filters.push(gtFilter(key, val, true));
        break;
      case '>':
        this._filters.push(gtFilter(key, val, false));
        break;
      case '<=':
        this._filters.push(ltFilter(key, val, true));
        break;
      case '<':
        this._filters.push(ltFilter(key, val, false));
        break;
      default:
        throw new Error('Invalid filter: ' + arg);
    }
  } else if (arg === '-j') {
    this._output = 'json';
  } else if (arg === '-t') {
    this._output = 'tab';
  } else if (arg === '-o') {
    this._nextIsOutput = true;
  } else if (this._nextIsOutput) {
    this._nextIsOutput = false;
    this._select.push.apply(this._select, arg.split(','));
  }
};

// The arg is a string, so just coerce both to strings
function exactFilter(key, val) {
  return function exact(obj) {
    return (''+obj[key]) === (''+val);
  };
}

function stringFilter(key, val) {
  return function string(obj) {
    return obj[key] && (''+obj[key]).indexOf(val) !== -1;
  };
}

function neqFilter(key, val) {
  return function neq(obj) {
    return obj[key] != val;
  };
}

function regexFilter(key, val) {
  val = new RegExp(val);
  return function regex(obj) {
    return obj[key] && (''+obj[key]).match(val);
  };
}

function gtFilter(key, val, eq) {
  val = +val;
  return eq ? function gte(obj) {
    return (+obj[key]) >= val;
  } : function gt(obj) {
    return (+obj[key]) > val;
  };
}

function ltFilter(key, val, eq) {
  val = +val;
  return eq ? function lte(obj) {
    return (+obj[key]) <= val;
  } : function lt(obj) {
    return (+obj[key]) < val;
  };
}

Search.prototype._transform = function(chunk, encoding, cb) {
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

Search.prototype._parseLine = function(line, index, lines) {
  if (!this._sawHead) {
    this._sawHead = true;

    // while we're here, also output a header line if we're in tab mode
    if (this._output === 'tab' && this._select.length > 0) {
      this.push(this._select.map(function (f) {
        return f.toUpperCase();
      }).join('\t') + '\n');
    }
    return;
  }

  var tsl = line.split(',');
  var ts = parseInt(tsl.shift(), 10);
  line = tsl.join(',');

  var obj = JSON.parse(line);
  var pass = true;
  this._filters.forEach(function(f) {
    pass = pass && f(obj);
  });
  if (!pass)
    return;

  if (this._select.length > 0) {
    obj = this._select.reduce(function(set, key) {
      set[key] = obj[key];
      return set;
    }, {});
  }

  var line;
  switch (this._output) {
    case 'tab':
      line = Object.keys(obj).map(function(k) {
        return obj[k];
      }).join('\t')
      break;
    case 'json':
      line = JSON.stringify(obj);
      break;
    default:
      throw new Error('Unknown output format: ' + this._output);
  }
  this.push(line + '\n', 'utf8');
}

if (module === require.main) {
  var search = new Search(process.argv.slice(2));
  process.stdin.pipe(search).pipe(process.stdout);
}
