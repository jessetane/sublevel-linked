var tape = require('tape')
var memup = require('memdb')
var Sublink = require('../')

var raw = memup()
var db = Sublink(raw)

var goodLock = Sublink.lock
var badLock = {
  read: function (cb) { cb() },
  write: function (cb) { cb() },
  unlock: function () {}
}

tape('show locks are needed', function (t) {
  t.plan(4)

  Sublink.lock = badLock

  var a = db
    .sublink('a')

  a.put('b', '42', function (err) {
    t.error(err)

    a.put('c', '42', onwrite)
    db.del('a', onwrite)
  })

  var i = 2
  function onwrite (err) {
    t.error(err)
    if (--i === 0) {
      var rs = raw.createReadStream()
      rs.on('data', function (data) {
        t.equal(data.key, Sublink.SEPARATOR + 'a' + Sublink.SEPARATOR + 'c')
      })
      rs.on('end', t.end.bind(t))
    }
  }
})

tape('locks work', function (t) {
  t.plan(3)

  Sublink.lock = goodLock

  var a = db
    .sublink('a')

  a.put('b', '42', function (err) {
    t.error(err)

    a.put('c', '42', onwrite)
    db.del('a', onwrite)
  })

  var i = 2
  function onwrite (err) {
    t.error(err)
    if (--i === 0) {
      var rs = raw.createReadStream()
      rs.on('data', function (data) {
        t.fail()
      })
      rs.on('end', t.end.bind(t))
    }
  }
})
