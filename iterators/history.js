module.exports = class HistoryIterator {
  constructor(batch, opts = {}) {
    this.batch = batch
    this.options = opts
    this.live = !!opts.live
    this.gte = 0
    this.lt = 0
    this.reverse = !!opts.reverse
    this.limit = typeof opts.limit === 'number' ? opts.limit : -1
    this.encoding = opts.encoding || batch.encoding
    // ring of in-flight block reads ahead of the cursor (bounded runs only)
    this._reads = this.live ? null : new Array(READAHEAD)
    this._next = 0
    if (this.live && this.reverse) {
      throw new Error('Cannot have both live and reverse enabled')
    }
  }

  async open() {
    await this.batch.getRoot(false) // does the update dance
    this.gte = gte(this.options, this.batch.version)
    this.lt = this.live ? Infinity : lt(this.options, this.batch.version)
    this._next = this.reverse ? this.lt - 1 : this.gte
  }

  async next() {
    if (this.limit === 0) return null
    if (this.limit > 0) this.limit--

    if (this.gte >= this.lt) return null

    if (this.reverse) {
      if (this.lt <= 1) return null
      return final(await this._read(--this.lt), this.encoding)
    }

    return final(await this._read(this.gte++), this.encoding)
  }

  _read(seq) {
    if (this._reads === null) return this.batch.getBlock(seq)

    // this.limit was already decremented for seq itself
    const ahead = this.limit === -1 ? READAHEAD : Math.min(READAHEAD, this.limit + 1)

    if (this.reverse) {
      const end = Math.max(this.gte, 1)
      while (this._next >= end && seq - this._next < ahead) this._fetch(this._next--)
    } else {
      while (this._next < this.lt && this._next - seq < ahead) this._fetch(this._next++)
    }

    const slot = seq & (READAHEAD - 1)
    const read = this._reads[slot]
    this._reads[slot] = null
    return read === null || read === undefined ? this.batch.getBlock(seq) : read
  }

  _fetch(seq) {
    const read = this.batch.getBlock(seq)
    read.catch(noop)
    this._reads[seq & (READAHEAD - 1)] = read
  }

  close() {
    return this.batch._closeSnapshot()
  }
}

const READAHEAD = 16 // power of two

function noop() {}

function final(node, encoding) {
  const type = node.isDeletion() ? 'del' : 'put'
  return { type, ...node.final(encoding) }
}

function gte(opts, version) {
  if (opts.gt) return (opts.gt < 0 ? opts.gt + version : opts.gt) + 1
  const gte = opts.gte || opts.since || 1
  return gte < 0 ? gte + version : gte
}

function lt(opts, version) {
  if (opts.lte === 0 || opts.lt === 0 || opts.end === 0) return 0
  if (opts.lte) return (opts.lte < 0 ? opts.lte + version : opts.lte) + 1
  const lt = opts.lt || opts.end || version
  return lt < 0 ? lt + version : lt
}
