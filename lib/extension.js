const { Extension } = require('./messages')

// const MAX_ACTIVE = 32
const FLUSH_BATCH = 128
const MAX_PASSIVE_BATCH = 2048
const MAX_ACTIVE_BATCH = MAX_PASSIVE_BATCH + FLUSH_BATCH

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
    if (len >= FLUSH_BATCH) {
      this.send()
      this.clear()
    }
  }

  send () {
    if (!this.blocks.length) return
    this.outgoing.send(Extension.encode({ cache: { blocks: this.blocks, start: this.start, end: this.end } }), this.from)
  }

  clear () {
    this.start = this.end = 0
    this.blocks = []
  }
}

class HyperbeeExtension {
  constructor (db) {
    this.encoding = null
    this.outgoing = null
    this.db = db
    this.active = 0
  }

  get (version, key) {
    this.outgoing.broadcast(Extension.encode({ get: { version, key } }))
  }

  iterator (snapshot) {
    if (snapshot.ended) return
    if (snapshot.limit === 0) return
    if (snapshot.limit === -1) snapshot.limit = 0
    this.outgoing.broadcast(Extension.encode({ iterator: snapshot }))
  }

  onmessage (buf, from) {
    // TODO: handle max active extension messages
    // this.active++

    const message = decode(buf)
    if (!message) return

    if (message.cache) this.oncache(message.cache, from)
    if (message.get) this.onget(message.get, from)
    if (message.iterator) this.oniterator(message.iterator, from)
  }

  oncache (message, from) {
    if (!message.blocks.length) return
    this.db.core.download(message)
  }

  onget (message, from) {
    if (!message.version || message.version > this.db.version) return

    const b = new Batch(this.outgoing, from)
    const db = this.db.checkout(message.version)

    db.get(message.key, { extension: false, wait: false, update: false, onseq }).then(done, done)

    function done () {
      db.close().catch(noop)
      b.send()
    }

    function onseq (seq) {
      b.push(seq)
    }
  }

  async oniterator (message, from) {
    if (!message.version || message.version > this.db.version) return

    const b = new Batch(this.outgoing, from)
    const seqs = new Set()

    let skip = message.checkpoint.length
    let work = 0

    const db = this.db.checkout(message.version)
    const ite = db.createRangeIterator({
      ...message,
      wait: false,
      extension: false,
      update: false,
      limit: message.limit === 0 ? -1 : message.limit,
      onseq (seq) {
        if (skip && skip--) return
        if (seqs.has(seq)) return
        work++
        seqs.add(seq)
        b.push(seq)
      }
    })

    try {
      await ite.open()
      // eslint-disable-next-line no-unmodified-loop-condition
      while (work < MAX_ACTIVE_BATCH) {
        if (!(await ite.next())) break
      }
    } catch (_) {
      // do nothing
    } finally {
      ite.close().catch(noop)
      db.close().catch(noop)
      b.send()
    }
  }

  static register (db) {
    const e = new this(db)
    e.outgoing = db.core.registerExtension('hyperbee', e)
    return e
  }
}

HyperbeeExtension.BATCH_SIZE = MAX_PASSIVE_BATCH

module.exports = HyperbeeExtension

function decode (buf) {
  try {
    return Extension.decode(buf)
  } catch (err) {
    return null
  }
}

function noop () {}
