# sublink
Like [level-sublevel](https://github.com/dominictarr/level-sublevel) but with links.

[![tests](https://img.shields.io/travis/jessetane/sublink.svg?style=flat-square&branch=master)](https://travis-ci.org/jessetane/sublink)

## Why
So you can make hierarchies.

## How
Keys are sorted as utf8. U+10FFFF (0xF4, 0x8F, 0xBF, 0xBF) is used as a separator (so you can use any unicode string as a key), and keys pointing to nested keyspaces are suffixed with U+0000 (0x00) so they can be detected without reading any value data.

## Example
```javascript
var memdown = require('memdown')
var levelup = require('levelup')
var Sublink = require('../')

var db = Sublink(levelup('/tmp/db', { db: memdown }))

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
```

## Install
```bash
$ npm install jessetane/sublink#2.0.0
```

## Test
```bash
$ npm run test
```

## Notes
* You must explicitly delete sublinks before attempting to overwrite them with raw values.
* You must explicitly delete raw values before attempting to overwrite them with sublinks.
* Atomicity is not guaranteed! Although unlikely, data corruption is probably possible because keys must be read (asynchronously) and checked for existence before updates can be written.
* Currently options are passed through to underlying levelup calls, however altering keyEncoding would break this module so maybe don't do that.

## License
WTFPL
