var tape = require('tape')
var memup = require('memdb')
var Sublink = require('../')

var raw = memup()
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
        key: 'a\x00',
        value: '\x00'
      }, {
        key: 'x',
        value: '42'
      }, {
        key: '~a~b\x00',
        value: '\x00'
      }, {
        key: '~a~~b~x',
        value: '42'
      }
    ].map(replaceSeparator)

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

tape('put / del super links', function (t) {
  t.plan(13)

  var sub = db
    .sublink('q')
    .sublink('w')
    .sublink('e')
    .sublink('r')

  sub.put('x', '42', function (err) {
    t.error(err)

    var rs = raw.createReadStream()
    var expected = [
      {
        key: 'q\x00',
        value: '\x00'
      }, {
        key: '~q~w\x00',
        value: '\x00'
      }, {
        key: '~q~~w~e\x00',
        value: '\x00'
      }, {
        key: '~q~~w~~e~r\x00',
        value: '\x00'
      }, {
        key: '~q~~w~~e~~r~x',
        value: '42'
      }
    ].map(replaceSeparator)

    var n = 0
    rs.on('data', function (chunk) {
      t.equal(chunk.key, expected[n].key)
      t.equal(chunk.value, expected[n++].value)
    })
    rs.on('end', function () {
      sub.del('x', function (err) {
        t.error(err)

        var rs = raw.createReadStream()
        rs.on('data', function (chunk) {
          t.fail()
        })
        rs.on('end', function (chunk) {
          t.pass()
        })
      })
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

function replaceSeparator (chunk) {
  chunk.key = chunk.key.replace(/~/g, Sublink.SEPARATOR)
  return chunk
}
