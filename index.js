module.exports = Sublevel

var thru = require('through2').obj

var SEPARATOR = Sublevel.SEPARATOR = '\uD83F\uDFFF' // unicode: 0x10FFFF, utf8: 0xF4 0x8F 0xBF 0xBF
var LINK_SUFFIX = Sublevel.LINK_SUFFIX = '\u0000'   // unicode: 0x00, utf8: 0x00

function Sublevel (levelup) {
  if (!(this instanceof Sublevel)) {
    return new Sublevel(levelup)
  }

  this._levelup = levelup
  this._path = []
  this._prefix = ''
}

Sublevel.prototype.toString = function () {
  var name = this._name ? ' ' + this._name : ''
  return '<Sublevel' + name + '>'
}

Sublevel.prototype.sublevel = function (name) {
  var sublevel = new Sublevel(this._levelup)
  sublevel._name = name
  sublevel._path = this._path.concat(name)
  sublevel._prefix = this._prefix + SEPARATOR + name + SEPARATOR
  return sublevel
}

Sublevel.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var batch = [
    {
      type: 'put',
      key: this._prefix + key,
      value: value
    }
  ]

  this._ensureLink(batch)

  var self = this
  this._levelup.get(this._prefix + key + LINK_SUFFIX, function (err) {
    if (err) {
      if (err.notFound) {
        self._levelup.batch(batch, opts, cb)
      } else {
        cb(err)
      }
    } else {
      self._del(key, function (err, delBatch) {
        if (err) return cb(err)
        batch = delBatch.concat(batch)
        self._levelup.batch(batch, opts, cb)
      })
    }
  })
}

Sublevel.prototype._ensureLink = function (batch) {
  if (this._prefix) {
    var prefix = ''
    for (var i = 0; i < this._path.length; i++) {
      var name = this._path[i]
      batch[batch.length] = {
        type: 'put',
        key: prefix + name + LINK_SUFFIX,
        value: LINK_SUFFIX
      }
      batch[batch.length] = {
        type: 'del',
        key: prefix + name
      }
      prefix += SEPARATOR + name + SEPARATOR
    }
  }
}

Sublevel.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  this._levelup.get(this._prefix + key + LINK_SUFFIX, function (err) {
    if (err) {
      if (err.notFound) {
        self._levelup.get(self._prefix + key, opts, cb)
      } else {
        cb(err)
      }
    } else {
      cb(null, self.sublevel(key))
    }
  })
}

Sublevel.prototype.del = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  this._levelup.get(this._prefix + key + LINK_SUFFIX, function (err) {
    if (err) {
      if (err.notFound) {
        self._levelup.del(self._prefix + key, opts, cb)
      } else {
        cb(err)
      }
    } else {
      self._del(key, function (err, batch) {
        if (err) return cb(err)
        self._levelup.batch(batch, cb)
      })
    }
  })
}

Sublevel.prototype._del = function (key, cb) {
  var batch = [
    {
      type: 'del',
      key: this._prefix + key + LINK_SUFFIX
    }, {
      type: 'del',
      key: this._prefix + key
    }
  ]
  var sublevelPrefix = this._prefix + SEPARATOR + key + SEPARATOR
  var ks = this._levelup.createKeyStream({
    start: sublevelPrefix,
    end: sublevelPrefix + SEPARATOR + SEPARATOR
  })

  ks.on('data', function (key) {
    batch[batch.length] = {
      type: 'del',
      key: key
    }
  })

  ks.on('error', function (err) {
    if (!cb._called) {
      cb._called = true
      cb(err)
    }
  })

  ks.on('end', function () {
    if (!cb._called) {
      cb(null, batch)
    }
  })
}

Sublevel.prototype.batch = function (batch, opts, cb) {
  var self = this
  var prebatch = []
  var hasPut = false
  var n = batch.length

  batch.forEach(function (chunk, i) {
    self._levelup.get(self._prefix + chunk.key + LINK_SUFFIX, function (err) {
      if (err) {
        if (err.notFound) {
          if (--n === 0) ready()
        } else if (!cb._called) {
          cb._called = true
          cb(err)
        }
      } else {
        self._del(chunk.key, function (err, delBatch) {
          if (err) {
            if (!cb._called) {
              cb._called = true
              cb(err)
            }
            return
          }
          prebatch = prebatch.concat(delBatch)
          if (--n === 0) ready()
        })
      }
    })

    batch[i] = {
      type: chunk.type,
      key: self._prefix + chunk.key,
      value: chunk.value
    }

    if (chunk.type === 'put') {
      hasPut = true
    }
  })

  function ready () {
    if (hasPut) self._ensureLink(batch)
    self._levelup.batch(prebatch.concat(batch), opts, cb)
  }
}

Sublevel.prototype.createKeyStream = function (opts) {
  opts = opts || {}
  var prefix = this._prefix
  var end = ''

  if (prefix) {
    if (opts.start) opts.start = prefix + opts.start
    else opts.start = prefix
    end = prefix
  }

  if (opts.end) opts.end = end + opts.end
  else opts.end = end
  opts.end += SEPARATOR

  var rs = this._levelup.createKeyStream(opts)
  var tr = thru(function (key, enc, cb) {
    if (prefix) {
      key = key.slice(prefix.length)
    }
    if (key.slice(-1)[0] === LINK_SUFFIX) {
      key = key.slice(0, -1)
    }
    cb(null, key)
  })
  rs.on('error', tr.emit.bind(tr, 'error'))
  rs.pipe(tr)

  return tr
}

Sublevel.prototype.createReadStream = function (opts) {
  opts = opts || {}
  var prefix = this._prefix
  var end = ''

  if (prefix) {
    if (opts.start) opts.start = prefix + opts.start
    else opts.start = prefix
    end = prefix
  }

  if (opts.end) opts.end = end + opts.end
  else opts.end = end
  opts.end += SEPARATOR

  var self = this
  var rs = this._levelup.createReadStream(opts)
  var tr = thru(function (chunk, enc, cb) {
    if (prefix) {
      chunk.key = chunk.key.slice(prefix.length)
    }
    if (chunk.key.slice(-1)[0] === LINK_SUFFIX) {
      chunk.key = chunk.key.slice(0, -1)
      chunk.value = self.sublevel(chunk.key)
    }
    cb(null, chunk)
  })
  rs.on('error', tr.emit.bind(tr, 'error'))
  rs.pipe(tr)

  return tr
}
