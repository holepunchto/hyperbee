module.exports = class HistoryIterator {
  constructor (batch, opts = {}) {
    this.batch = batch
    this.options = opts
    this.live = !!opts.live
    this.gte = 0
    this.lt = 0
    this.reverse = !!opts.reverse
    this.limit = typeof opts.limit === 'number' ? opts.limit : -1
    this.encoding = opts.encoding || batch.encoding
    if (this.live && this.reverse) {
      throw new Error('Cannot have both live and reverse enabled')
    }
  }

  async open () {
    await this.batch.getRoot(false) // does the update dance
    this.gte = gte(this.options, this.batch.version)
    this.lt = this.live ? Infinity : lt(this.options, this.batch.version)
  }

  async next () {
    if (this.limit === 0) return null
    if (this.limit > 0) this.limit--

    if (this.gte >= this.lt) return null

    if (this.reverse) {
      if (this.lt <= 1) return null
      return final(await this.batch.getBlock(--this.lt), this.encoding)
    }

    return final(await this.batch.getBlock(this.gte++), this.encoding)
  }

  close () {
    return this.batch._closeSnapshot()
  }
}

function final (node, encoding) {
  const type = node.isDeletion() ? 'del' : 'put'
  return { type, ...node.final(encoding) }
}

function gte (opts, version) {
  if (opts.gt) return (opts.gt < 0 ? (opts.gt + version) : opts.gt) + 1
  const gte = opts.gte || opts.since || 1
  return gte < 0 ? gte + version : gte
}

function lt (opts, version) {
  if (opts.lte === 0 || opts.lt === 0 || opts.end === 0) return 0
  if (opts.lte) return (opts.lte < 0 ? (opts.lte + version) : opts.lte) + 1
  const lt = opts.lt || opts.end || version
  return lt < 0 ? lt + version : lt
}
