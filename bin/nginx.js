// ask nginx where its config file is, and then parse the config for log info
var spawn = require('child_process').spawn;
var ncp = require('nginx-config-parser');
var fs = require('fs');
var path = require('path');

var ngx = spawn('nginx', ['-t']);
ngx.stderr.setEncoding('utf8');
var c = '';
ngx.stderr.on('data', function(chunk) {
  c += chunk;
});
ngx.stderr.on('end', function() {
  var p = c.match(/^nginx: the configuration file (.*)/);
  if (!p || !p[1] || !p[1].match(/ syntax is ok$/)) {
    console.error(c);
    process.exit(1);
  }
  p = p[1].replace(/ syntax is ok$/, '');
  getRoot(function(root) {
    conf(p, root);
  })
});

function getRoot(cb) {
  var ngx = spawn('nginx', ['-V']);
  ngx.stderr.setEncoding('utf8');
  var c = '';
  ngx.stderr.on('data', function(chunk) {
    c += chunk;
  });
  ngx.stderr.on('end', function() {
    var root = c.split(/\n/).filter(function(l) {
      return l.match(/^configure arguments/);
    })[0].split(' ').filter(function(a) {
      return a.match(/^--prefix=/);
    })[0].replace(/^--prefix=/, '');
    cb(root);
  });
}

function conf(cfile, root) {
  var config = ncp.queryFromString(fs.readFileSync(cfile, 'utf-8'));
  config[0].http.forEach(function(http) {
    // get all the log formats.
    var formats = {};
    if (http.log_format) {
      http.log_format.forEach(function(lf) {
        var name = lf.shift();
        var val = lf.map(function(v) {
          return v.replace(/^['"]/, '').replace(/['"]$/, '');
        }).join('');
        formats[name] = val;
      });
    }

    var logs = {};
    if (http.access_log) {
      http.access_log.forEach(function(al) {
        var file = al[0];
        if (file.charAt(0) !== '/')
          file = path.resolve(root, file);
        var type = al[1];
        logs[file] = type;
      });
    }

    if (http.server) {
      http.server.forEach(function(server) {
        if (server.access_log) {
          server.access_log.forEach(function(al) {
            var file = al[0];
            var type = al[1];
            logs[file] = type;
          });
        }
      });
    }

    Object.keys(logs).forEach(function(file) {
      logs[file] = formats[logs[file]];
    });
    console.error(logs);
  });
}
