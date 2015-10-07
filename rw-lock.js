// stolen from https://github.com/Wizcorp/locks

module.exports = ReadWriteLock

function ReadWriteLock () {
  this.isLocked = null
  this._readLocks = 0
  this._waitingToRead = []
  this._waitingToWrite = []
  this.unlock = this.unlock.bind(this)
}

ReadWriteLock.prototype.read = function (cb) {
  if (this.isLocked === 'W') {
    this._waitingToRead.push(cb)
  } else {
    this._readLocks += 1
    this.isLocked = 'R'
    cb()
  }
}

ReadWriteLock.prototype.write = function (cb) {
  if (this.isLocked) {
    this._waitingToWrite.push(cb)
  } else {
    this.isLocked = 'W'
    cb()
  }
}

ReadWriteLock.prototype.unlock = function () {
  var waiter

  if (this.isLocked === 'R') {
    this._readLocks -= 1

    if (this._readLocks === 0) {
      waiter = this._waitingToWrite.shift()
      if (waiter) {
        this.isLocked = 'W'
        waiter()
      } else {
        this.isLocked = null
      }
    }
  } else if (this.isLocked === 'W') {
    var rlen = this._waitingToRead.length

    if (rlen === 0) {
      waiter = this._waitingToWrite.shift()
      if (waiter) {
        this.isLocked = 'W'
        waiter()
      } else {
        this.isLocked = null
      }
    } else {
      this.isLocked = 'R'
      this._readLocks = rlen

      var waiters = this._waitingToRead.slice()
      this._waitingToRead = []

      for (var i = 0; i < rlen; i++) {
        waiters[i]()
      }
    }
  } else {
    throw new Error('ReadWriteLock is not locked')
  }
}
