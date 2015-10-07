var memup = require('memdb')
var Sublink = require('../')

var db = Sublink(memup())

db.put('x', '42', function () {
  var y = db.sublink('y')
  y.put('x', '42', function () {
    var rs = db.createReadStream()
    rs.on('data', function (chunk) {
      console.log(chunk.key + ': ' + chunk.value)
      // should print
      // x: 42
      // y: <Sublink y>
    })
  })
})
