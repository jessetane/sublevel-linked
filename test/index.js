var tape = require('tape')
var memdown = require('memdown')
var levelup = require('levelup')
var Sublink = require('../')

var raw = levelup('/tmp/db', { db: memdown })
var db = Sublink(raw)

tape('put', function (t) {
  t.plan(3)

  db.put('x', '42', function (err) {
    t.error(err)
    raw.get('x', function (err, value) {
      t.error(err)
      t.equal(value, '42')
    })
  })
})

tape('get', function (t) {
  t.plan(2)

  db.get('x', function (err, value) {
    t.error(err)
    t.equal(value, '42')
  })
})

tape('put key to be overridden by link', function (t) {
  t.plan(1)

  db.put('a', '42', function (err) {
    t.error(err)
  })
})

tape('put to sublink', function (t) {
  t.plan(7)

  var sub = db.sublink('a')
  sub.put('x', '42', function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a' + Sublink.LINK_SUFFIX,
        value: Sublink.LINK_SUFFIX
      }, {
        key: 'x',
        value: '42'
      }, {
        key: Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + 'x',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})

tape('get from sublink', function (t) {
  t.plan(2)

  var sub = db.sublink('a')
  sub.get('x', function (err, value) {
    t.error(err)
    t.equal(value, '42')
  })
})

tape('put to sub-sublink', function (t) {
  t.plan(1)

  var sub = db.sublink('a').sublink('a')
  sub.put('x', '42', function (err) {
    t.error(err)
  })
})

tape('keyStream from sublink shows link to sublink', function (t) {
  t.plan(2)

  var sub = db.sublink('a')
  var rs = sub.createKeyStream()
  var expected = [ 'a', 'x' ]

  var n = 0
  rs.on('data', function (key) {
    t.equal(key, expected[n++])
  })
})

tape('readStream from top level shows link to sublink', function (t) {
  t.plan(4)

  var rs = db.createReadStream()
  var expected = [
    {
      key: 'a',
      constructor: Sublink
    }, {
      key: 'x',
      constructor: String
    }
  ]

  var n = 0
  rs.on('data', function (chunk) {
    t.equal(chunk.key, expected[n].key)
    t.ok(chunk.value.constructor === expected[n++]['constructor'])
  })
})

tape('put to top level link removes corresponding sublinks', function (t) {
  t.plan(5)

  db.put('a', '42', function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a',
        value: '42'
      }, {
        key: 'x',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})

tape('batch', function (t) {
  t.plan(7)

  var sub = db.sublink('a')
  sub.batch([
    {
      type: 'put',
      key: 'x',
      value: '42'
    }
  ], function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a' + Sublink.LINK_SUFFIX,
        value: Sublink.LINK_SUFFIX
      }, {
        key: 'x',
        value: '42'
      }, {
        key: Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + 'x',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})

tape('batch removes overridden sublinks', function (t) {
  t.plan(3)

  db.batch([
    {
      type: 'put',
      key: 'a',
      value: '42'
    }, {
      type: 'del',
      key: 'x'
    }
  ], function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})

tape('ensure links', function (t) {
  t.plan(13)

  var sub = db
    .sublink('q')
    .sublink('w')
    .sublink('e')
    .sublink('r')

  sub.put('x', '42', function (err) {
    t.error(err)

    var S = Sublink.SEPARATOR
    var L = Sublink.LINK_SUFFIX
    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a',
        value: '42'
      }, {
        key: 'q' + L,
        value: L
      }, {
        key: S + 'q' + S + 'w' + L,
        value: L
      }, {
        key: S + 'q' + S + S + 'w' + S + 'e' + L,
        value: L
      }, {
        key: S + 'q' + S + S + 'w' + S + S + 'e' + S + 'r' + L,
        value: L
      }, {
        key: S + 'q' + S + S + 'w' + S + S + 'e' + S + S + 'r' + S + 'x',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})

tape('del removes sublinks', function (t) {
  t.plan(3)

  db.del('q', function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'a',
        value: '42'
      }
    ]

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
  })
})
