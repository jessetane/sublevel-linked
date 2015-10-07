var tape = require('tape')
var memup = require('memdb')
var Sublink = require('../')

var raw = memup()
var db = Sublink(raw)

tape('event ordering', function (t) {
  t.plan(12)

  var n = 0
  raw.on('batch', onbatch)

  function onbatch (batch) {
    if (++n === 1) {
      t.deepEqual(batch, [
        { type: 'put', key: '~a~~b~x', value: '42' },
        { type: 'put', key: '~a~b\x00', value: '\x00' },
        { type: 'put', key: 'a\x00', value: '\x00' }
      ].map(replaceSeparator))
    } else if (n === 2) {
      t.deepEqual(batch, [
        { type: 'put', key: '~a~x', value: '42' }
      ].map(replaceSeparator))
    } else if (n === 3) {
      t.deepEqual(batch, [
        { type: 'put', key: '~a~~b~y', value: '42' }
      ].map(replaceSeparator))
    } else if (n === 4) {
      t.deepEqual(batch, [
        { type: 'del', key: '~a~~b~x' }
      ].map(replaceSeparator))
    } else if (n === 5) {
      t.deepEqual(batch, [
        { type: 'del', key: '~a~~b~y' },
        { type: 'del', key: '~a~b\x00' }
      ].map(replaceSeparator))
    } else if (n === 6) {
      t.deepEqual(batch, [
        { type: 'del', key: '~a~x' },
        { type: 'del', key: 'a\x00' }
      ].map(replaceSeparator))
    } else {
      t.fail()
    }
  }

  var a = db.sublink('a')
  var b = a.sublink('b')

  b.put('x', '42', function (err) {
    t.error(err)

    a.put('x', '42', function (err) {
      t.error(err)

      b.put('y', '42', function (err) {
        t.error(err)

        b.del('x', function (err) {
          t.error(err)

          b.del('y', function (err) {
            t.error(err)

            a.del('x', function (err) {
              t.error(err)
              raw.removeListener('batch', onbatch)
            })
          })
        })
      })
    })
  })
})

tape('batch event ordering', function (t) {
  t.plan(6)

  var n = 0
  raw.on('batch', onbatch)

  function onbatch (batch) {
    if (++n === 1) {
      t.deepEqual(batch, [
        { type: 'put', key: '~a~~b~c', value: '42' },
        { type: 'put', key: '~a~b\x00', value: '\x00' },
        { type: 'put', key: 'a\x00', value: '\x00' }
      ].map(replaceSeparator))
    } else if (n === 2) {
      t.deepEqual(batch, [
        { type: 'put', key: '~a~~b~d', value: '42' },
        { type: 'del', key: '~a~~b~c' }
      ].map(replaceSeparator))
    } else if (n === 3) {
      t.deepEqual(batch, [
        { type: 'del', key: '~a~~b~d' },
        { type: 'del', key: '~a~b\x00' },
        { type: 'del', key: 'a\x00' }
      ].map(replaceSeparator))
    } else {
      t.fail()
    }
  }

  var b = db
    .sublink('a')
    .sublink('b')

  b.batch([
    { type: 'put', key: 'c', value: '42' }
  ], function (err) {
    t.error(err)

    b.batch([
      { type: 'put', key: 'd', value: '42' },
      { type: 'del', key: 'c' }
    ], function (err) {
      t.error(err)

      db.batch([
        { type: 'del', key: 'a' }
      ], function (err) {
        t.error(err)
        raw.removeListener('batch', onbatch)
      })
    })
  })
})

function replaceSeparator (chunk) {
  chunk.key = chunk.key.replace(/~/g, Sublink.SEPARATOR)
  return chunk
}
