# sublink
Like [level-sublevel](https://github.com/dominictarr/level-sublevel) but with links to nested keyspaces.

[![tests](https://img.shields.io/travis/jessetane/sublink.svg?style=flat-square&branch=master)](https://travis-ci.org/jessetane/sublink)

## Why
So you can make traversable hierarchies.

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
$ npm install sublink
```

## Test
```bash
$ npm run test
```

## Notes
* Atomicity is not guaranteed! Although unlikely, data corruption is probably possible because keys must be read (asynchronously) and checked for existence before updates can be written.
* Currently options are passed through to underlying levelup calls, however altering keyEncoding would break this module so maybe don't do that.

## Changelog
#### 3.0.0
* Don't require explictly deletes before overwrites - higher level modules can handle this
* Expose public `name` property
* Add public `superlink` method
#### 2.0.0
* Require explicitly deleting sublinks before overwriting with a value and vice-versa
#### 1.0.0
* First working version

## License
WTFPL
