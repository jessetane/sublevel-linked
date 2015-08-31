var memdown = require('memdown')
var levelup = require('levelup')
var Sublevel = require('../')

var db = Sublevel(levelup('/tmp/db', { db: memdown }))

db.put('x', '42', function () {
  var y = db.sublevel('y')
  y.put('x', '42', function () {
    var rs = db.createReadStream()
    rs.on('data', function (chunk) {
      console.log(chunk.key + ': ' + chunk.value)
      // should print
      // x: 42
      // y: <Sublevel y>
    })
  })
})
