module.exports = Sublink

var ReadWriteLock = require('./rw-lock')
var TransformStream = require('stream').Transform

function thru (f) {
  var tr = new TransformStream({ objectMode: true })
  tr._transform = f
  return tr
}

var SEPARATOR = Sublink.SEPARATOR = '\uD83F\uDFFF' // unicode: 0x10FFFF, utf8: 0xF4 0x8F 0xBF 0xBF
var LINK_SUFFIX = Sublink.LINK_SUFFIX = '\u0000'   // unicode: 0x00, utf8: 0x00

Sublink.lock = new ReadWriteLock()

function Sublink (levelup) {
  if (!(this instanceof Sublink)) {
    return new Sublink(levelup)
  }

  if (levelup instanceof Sublink) {
    this._levelup = levelup._levelup
    this.name = levelup.name
    this._path = levelup._path.slice()
    this._prefix = levelup._prefix
  } else {
    this._levelup = levelup
    this.name = ''
    this._path = []
    this._prefix = ''
  }
}

Sublink.prototype.toString = function () {
  var name = this.name ? ' ' + this.name : ''
  return '<Sublink' + name + '>'
}

Sublink.prototype.sublink = function (name) {
  var sublink = new Sublink(this._levelup)
  sublink.name = name
  sublink._path = this._path.concat(name)
  sublink._prefix = this._prefix + SEPARATOR + name + SEPARATOR
  return sublink
}

Sublink.prototype.superlink = function () {
  if (this._path.length === 0) return this
  var sublink = new Sublink(this._levelup)
  if (this._path.length > 1) {
    sublink._path = this._path.slice(0, -1)
    sublink._prefix = this._prefix.slice(0, -(this.name.length + SEPARATOR.length * 2))
    sublink.name = sublink._path.slice(-1)[0]
  }
  return sublink
}

Sublink.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  var isSubOperation = opts && opts.suboperation
  if (isSubOperation) {
    doread()
  } else {
    Sublink.lock.read(doread)
  }

  function doread () {
    self._levelup.get(self._prefix + key + LINK_SUFFIX, function (err) {
      if (err) {
        if (err.notFound) {
          self._levelup.get(self._prefix + key, opts, cbwrap)
        } else {
          cbwrap(err)
        }
      } else {
        cbwrap(null, self.sublink(key))
      }
    })
  }

  function cbwrap () {
    if (!isSubOperation) Sublink.lock.unlock()
    cb.apply(null, arguments)
  }
}

Sublink.prototype.put = function (key, value, opts, cb) {
  this._write('put', key, value, opts, cb)
}

Sublink.prototype.del = function (key, opts, cb) {
  this._write('del', key, null, opts, cb)
}

Sublink.prototype._write = function (type, key, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }

  var self = this
  var isSubOperation = opts && opts.suboperation
  if (isSubOperation) {
    dochecks()
  } else {
    Sublink.lock.write(dochecks)
  }

  function dochecks () {
    self._isLink(key, function (err, isLink) {
      if (err) return cbwrap(err)

      var batch = [
        {
          type: type,
          key: self._prefix + key
        }
      ]

      if (type === 'put') {
        batch[0].value = value
      } else if (isLink) {
        batch = []
      }

      if (isLink) {
        self._delLink(key, function (err, delBatch) {
          if (err) return cbwrap(err)
          dowrite(delBatch.concat(batch))
        })
      } else {
        dowrite(batch)
      }
    })
  }

  function dowrite (batch) {
    if (isSubOperation) {
      cb(null, batch)
    } else {
      self['_' + type + 'Superlinks'](function (err, superBatch) {
        if (err) return cbwrap(err)
        batch = batch.concat(superBatch)
        self._levelup.batch(batch, opts, cbwrap)
      })
    }
  }

  function cbwrap () {
    if (!isSubOperation) Sublink.lock.unlock()
    cb.apply(null, arguments)
  }
}

Sublink.prototype.batch = function (batch, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = undefined
  }
  opts = opts || {}
  opts.suboperation = true

  var self = this
  var tmpBatch = []
  var n = batch.length

  Sublink.lock.write(function () {
    batch.forEach(function (chunk, i) {
      if (chunk.type === 'put') {
        self.put(chunk.key, chunk.value, opts, onbatch)
      } else if (chunk.type === 'del') {
        self.del(chunk.key, opts, onbatch)
      }
    })
  })

  function onbatch (err, _batch) {
    if (cbwrap._called) return
    if (err) {
      cbwrap._called = true
      return cbwrap(err)
    }
    tmpBatch = tmpBatch.concat(_batch)
    if (--n === 0) {
      batch = tmpBatch
      tmpBatch = []
      var i = batch.length
      var keys = {}
      var type = 'del'
      while (i-- > 0) {
        var op = batch[i]
        if (!keys[op.key]) {
          keys[op.key] = true
          tmpBatch[tmpBatch.length] = op
          if (type === 'del' && op.type === 'put') {
            type = 'put'
          }
        }
      }
      self['_' + type + 'Superlinks'](function (err, superBatch) {
        if (err) return cbwrap(err)
        batch = tmpBatch.reverse().concat(superBatch)
        self._levelup.batch(batch, opts, cbwrap)
      })
    }
  }

  function cbwrap () {
    Sublink.lock.unlock()
    cb.apply(null, arguments)
  }
}

Sublink.prototype.createKeyStream = function (opts) {
  opts = opts || {}
  opts.values = false
  return this.createReadStream(opts)
}

Sublink.prototype.createReadStream = function (opts) {
  opts = opts || {}
  opts.values = opts.values !== false

  var self = this
  var prefix = this._prefix
  var end = ''

  if (prefix) {
    if (opts.gte) opts.gte = prefix + opts.gte
    else opts.gte = prefix
    end = prefix
  }

  if (opts.lte) opts.lte = end + opts.lte
  else opts.lte = end
  opts.lte += SEPARATOR

  var tr = thru(opts.values ? filterKeysAndValues : filterKeys)

  if (opts.suboperation) {
    doread()
  } else {
    Sublink.lock.read(doread)
  }

  function filterKeys (key, enc, cb) {
    if (prefix) {
      key = key.slice(prefix.length)
    }
    if (key.slice(-1)[0] === LINK_SUFFIX) {
      key = key.slice(0, -1)
    }
    cb(null, key)
  }

  function filterKeysAndValues (chunk, enc, cb) {
    if (prefix) {
      chunk.key = chunk.key.slice(prefix.length)
    }
    if (chunk.key.slice(-1)[0] === LINK_SUFFIX) {
      chunk.key = chunk.key.slice(0, -1)
      chunk.value = self.sublink(chunk.key)
    }
    cb(null, chunk)
  }

  function doread () {
    var rs = self._levelup.createReadStream(opts)
    rs.on('error', tr.emit.bind(tr, 'error'))
    if (!opts.suboperation) {
      rs.on('end', Sublink.lock.unlock)
    }
    rs.pipe(tr)
  }

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

Sublink.prototype._putSuperlinks = function (cb, batch) {
  batch = batch || []
  var self = this
  var superlink = this.superlink()
  if (superlink === this) return cb(null, batch)
  superlink.get(this.name, { suboperation: true }, function (err, value) {
    if (err && !err.notFound) {
      return cb(err)
    }
    if (value instanceof Sublink) {
      return cb(null, batch)
    }
    if (!err) {
      batch[batch.length] = {
        type: 'del',
        key: superlink._prefix + self.name
      }
    }
    batch[batch.length] = {
      type: 'put',
      key: superlink._prefix + self.name + LINK_SUFFIX,
      value: LINK_SUFFIX
    }
    superlink._putSuperlinks(cb, batch)
  })
}

Sublink.prototype._delSuperlinks = function (cb, batch) {
  batch = batch || []
  var self = this
  var superlink = this.superlink()
  if (superlink === this) return cb(null, batch)
  var keys = -1
  var ks = this.createKeyStream({ limit: 2, suboperation: true })
  ks.on('error', function (err) {
    if (cb._called) return
    cb._called = true
    cb(err)
  })
  ks.on('data', function (key) {
    keys++
  })
  ks.on('end', function () {
    if (cb._called) return
    if (keys > 0) {
      cb._called = true
      return cb(null, batch)
    }
    batch[batch.length] = {
      type: 'del',
      key: superlink._prefix + self.name + LINK_SUFFIX
    }
    superlink._delSuperlinks(cb, batch)
  })
}

Sublink.prototype._delLink = function (key, cb) {
  var batch = [
    {
      type: 'del',
      key: this._prefix + key + LINK_SUFFIX
    }
  ]
  var sublinkPrefix = this._prefix + SEPARATOR + key + SEPARATOR
  var ks = this._levelup.createKeyStream({
    gte: sublinkPrefix,
    lte: sublinkPrefix + SEPARATOR + SEPARATOR
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
      cb(null, batch.reverse())
    }
  })
}
