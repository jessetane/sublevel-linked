# sublevel-linked
Like level-sublevel but with links to descending sublevels.

[![tests](https://img.shields.io/travis/jessetane/sublevel-linked.svg?style=flat-square&branch=master)](https://travis-ci.org/jessetane/sublevel-linked)

## Why
[level-sublevel](https://github.com/dominictarr/level-sublevel) is awesome, but does not provide any link between key names in a sublevel and the names of descending sublevels.

## How
Keys are treated as utf8. U+10FFFF (0xF4, 0x8F, 0xBF, 0xBF) is used as the separator (so you can use any unicode string as a key), and keys pointing to descending sublevels are suffixed with U+0000 (0x00) so they can be detected without reading value data.

## Example
```javascript
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
```

## Install
```bash
$ npm install jessetane/sublevel-linked#1.0.0
```

## Test
```bash
$ npm run test
```

## Notes
* Currently options are passed through to underlying levelup calls, however altering keyEncoding would break this module so maybe don't do that.
* A `batch` implementation is included for compatibility with levelup, but it doesn't guarantee atomicity! To work, it has to read keys from the current sublevel in order to ensure they aren't links to other sublevels - these read operations are asynchronous and therefore by the time your batch is ready to be executed, the database may have changed.

## License
WTFPL
