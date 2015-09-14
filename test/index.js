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

tape('put to sublink', function (t) {
  t.plan(1)

  db
    .sublink('a')
    .put('b', '42', function (err) {
    t.error(err)
  })
})

tape('get from sublink', function (t) {
  t.plan(2)

  db
    .sublink('a')
    .get('b', function (err, value) {
    t.error(err)
    t.equal(value, '42')
  })
})

tape('put to sublink overwrites existing value', function (t) {
  t.plan(9)

  db
    .sublink('a')
    .sublink('b')
    .put('x', '42', function (err) {
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
        key: Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + 'b' + Sublink.LINK_SUFFIX,
        value: Sublink.LINK_SUFFIX
      }, {
        key: Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + Sublink.SEPARATOR + 'b' + Sublink.SEPARATOR + 'x',
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

tape('keyStream from sublink shows link to sublink', function (t) {
  t.plan(1)

  var rs = db
    .sublink('a')
    .createKeyStream()

  var expected = [ 'b' ]

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

tape('put overwrites sublinks', function (t) {
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
  t.plan(6)

  db
    .sublink('b')
    .put('x', '42', function (err) {
    t.error(err)

    var batch = [
      {
        type: 'put',
        key: 'b',
        value: '42'
      }, {
        type: 'del',
        key: 'x'
      }
    ]

    db
      .batch(batch, function (err) {
      t.error(err)

      var rs = raw.createReadStream()
      var expected = [
        {
          key: 'a',
          value: '42'
        }, {
          key: 'b',
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
})

tape('del', function (t) {
  t.plan(4)

  db
    .sublink('b')
    .put('x', '42', function (err) {
    t.error(err)

    db.del('a', function (err) {
      t.error(err)

      db.del('b', function (err) {
        t.error(err)

        var rs = raw.createReadStream()
        rs.on('data', function (chunk) {
          t.fail()
        })
        rs.on('end', function () {
          t.pass()
        })
      })
    })
  })
})

tape('ensure parent links', function (t) {
  t.plan(11)

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

tape('pass in sublink as levelup', function (t) {
  t.plan(3)

  var sub = Sublink(db.sublink('a').sublink('b'))
  t.equal(sub.name, 'b')
  t.equal(sub._path.length, 2)
  t.equal(sub._prefix, Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + Sublink.SEPARATOR + 'b' + Sublink.SEPARATOR)
})

tape('superlink', function (t) {
  t.plan(3)

  var sub = db
    .sublink('a')
    .sublink('b')
    .superlink()
    .superlink()

  t.equal(sub.name, '')
  t.equal(sub._path.length, 0)
  t.equal(sub._prefix, '')
})
