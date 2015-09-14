module.exports = Sublink

var SEPARATOR = Sublink.SEPARATOR = '\uD83F\uDFFF' // unicode: 0x10FFFF, utf8: 0xF4 0x8F 0xBF 0xBF
var LINK_SUFFIX = Sublink.LINK_SUFFIX = '\u0000'   // unicode: 0x00, utf8: 0x00

var TransformStream = require('stream').Transform

function thru (f) {
  var tr = new TransformStream({ objectMode: true })
  tr._transform = f
  return tr
}

function Sublink (levelup) {
  if (!(this instanceof Sublink)) {
    return new Sublink(levelup)
  }

  this._levelup = levelup
  this._path = []
  this._prefix = ''
}

Sublink.prototype.toString = function () {
  var name = this._name ? ' ' + this._name : ''
  return '<Sublink' + name + '>'
}

Sublink.prototype.sublink = function (name) {
  var sublink = new Sublink(this._levelup)
  sublink._name = name
  sublink._path = this._path.concat(name)
  sublink._prefix = this._prefix + SEPARATOR + name + SEPARATOR
  return sublink
}

Sublink.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  this._isLink(key, function (err, isLink) {
    if (err) return cb(err)

    var batch = [
      {
        type: 'put',
        key: self._prefix + key,
        value: value
      }
    ]

    if (isLink) {
      self._delLink(key, function (err, delBatch) {
        if (err) return cb(err)
        doPut(delBatch.concat(batch))
      })
    } else {
      doPut(batch, cb)
    }
  })

  function doPut (batch) {
    self._ensureParentLinks(batch, function (err) {
      if (err) return cb(err)
      self._levelup.batch(batch, opts, cb)
    })
  }
}

Sublink.prototype.get = function (key, opts, cb) {
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
      cb(null, self.sublink(key))
    }
  })
}

Sublink.prototype.del = function (key, opts, cb) {
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
      self._delLink(key, function (err, batch) {
        if (err) return cb(err)
        self._levelup.batch(batch, cb)
      })
    }
  })
}

Sublink.prototype.batch = function (batch, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  var delBatch = []
  var hasPut = false
  var n = batch.length

  batch.forEach(function (chunk, i) {
    self._isLink(chunk.key, function (err, isLink) {
      if (cb._called) return
      if (err) {
        cb._called = true
        return cb(err)
      }
      hasPut = hasPut || chunk.type === 'put'
      batch[i] = {
        type: chunk.type,
        key: self._prefix + chunk.key,
        value: chunk.value
      }
      if (isLink) {
        self._delLink(chunk.key, function (err, _delBatch) {
          if (cb._called) return
          if (err) {
            cb._called = true
            return cb(err)
          }
          delBatch = delBatch.concat(_delBatch)
          if (--n === 0) ready()
        })
      } else {
        if (--n === 0) ready()
      }
    })
  })

  function ready () {
    if (hasPut) {
      self._ensureParentLinks(batch, function (err) {
        if (err) return cb(err)
        self._levelup.batch(delBatch.concat(batch), opts, cb)
      })
    } else {
      self._levelup.batch(delBatch.concat(batch), opts, cb)
    }
  }
}

Sublink.prototype.createKeyStream = function (opts) {
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

Sublink.prototype.createReadStream = function (opts) {
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
      chunk.value = self.sublink(chunk.key)
    }
    cb(null, chunk)
  })
  rs.on('error', tr.emit.bind(tr, 'error'))
  rs.pipe(tr)
  return tr
}

Sublink.prototype._isLink = function (key, cb) {
  var isLink = false
  this._levelup.get(this._prefix + key + LINK_SUFFIX, function (err) {
    if (err) {
      if (err.notFound) {
        err = null
      }
    } else {
      isLink = true
    }
    cb(err, isLink)
  })
}

Sublink.prototype._ensureParentLinks = function (batch, cb) {
  if (!this._prefix) {
    return cb()
  }

  var self = this
  var prefix = ''
  var n = this._path.length

  this._path.forEach(function (name) {
    var localPrefix = prefix
    prefix += SEPARATOR + name + SEPARATOR
    self._levelup.get(localPrefix + name, function (err) {
      if (cb._called) return
      if (err) {
        if (!err.notFound) {
          cb._called = true
          return cb(err)
        }
      } else {
        batch[batch.length] = {
          type: 'del',
          key: localPrefix + name
        }
      }
      batch[batch.length] = {
        type: 'put',
        key: localPrefix + name + LINK_SUFFIX,
        value: LINK_SUFFIX
      }
      if (--n === 0) cb()
    })
  })
}

Sublink.prototype._delLink = function (key, cb) {
  var batch = [
    {
      type: 'del',
      key: this._prefix + key + LINK_SUFFIX
    }, {
      type: 'del',
      key: this._prefix + key
    }
  ]
  var sublinkPrefix = this._prefix + SEPARATOR + key + SEPARATOR
  var ks = this._levelup.createKeyStream({
    start: sublinkPrefix,
    end: sublinkPrefix + SEPARATOR + SEPARATOR
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
