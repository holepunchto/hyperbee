const { Extension } = require('./messages')

class Batch {
  constructor (outgoing, from) {
    this.blocks = []
    this.start = 0
    this.end = 0
    this.outgoing = outgoing
    this.from = from
  }

  push (seq) {
    const len = this.blocks.push(seq)
    if (len === 1 || seq < this.start) this.start = seq
    if (len === 1 || seq >= this.end) this.end = seq + 1
  }

  send () {
    if (!this.blocks.length) return
    this.outgoing.send(Extension.encode({ cache: { blocks: this.blocks, start: this.start, end: this.end } }), this.from)
  }
}

module.exports = class HyperbExtension {
  constructor (db) {
    this.encoding = null
    this.outgoing = null
    this.db = db
    this.active = 0
  }

  get (head, key) {
    this.outgoing.broadcast(Extension.encode({ get: { head, key } }))
  }

  onmessage (buf, from) {
    const message = decode(buf)
    if (!message) return

    if (message.cache) this.oncache(message.cache, from)
    if (message.get) this.onget(message.get, from)
  }

  oncache (message, from) {
    if (!message.blocks.length) return

    this.db.feed.download(message)
  }

  onget (message, from) {
    const b = new Batch(this.outgoing, from)
    const done = b.send.bind(b)
    this.db.get(message.key, { extension: false, wait: false, update: false, onseq }).then(done, done)

    function onseq (seq) {
      b.push(seq)
    }
  }

  static register (db) {
    const e = new this(db)
    e.outgoing = db.feed.registerExtension('hyperbee', e)
    return e
  }
}

function decode (buf) {
  try {
    return Extension.decode(buf)
  } catch (err) {
    return null
  }
}

function noop () {}
