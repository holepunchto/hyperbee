module.exports = class HistoryIterator {
  constructor (db, opts = {}) {
    this.db = db
    this.options = opts
    this.live = !!opts.live
    this.gte = 0
    this.lt = 0
    this.reverse = !!opts.reverse
    this.limit = typeof opts.limit === 'number' ? opts.limit : -1
    if (this.live && this.reverse) {
      throw new Error('Cannot have both live and reverse enabled')
    }
  }

  async open () {
    await this.db.getRoot() // does the update dance
    this.gte = gte(this.options, this.db.version)
    this.lt = this.live ? Infinity : lt(this.options, this.db.version)
  }

  async next () {
    if (this.limit === 0) return null
    if (this.limit > 0) this.limit--

    if (this.gte >= this.lt) return null

    if (this.reverse) {
      if (this.lt <= 1) return null
      return (await this.db.getBlock(--this.lt, this.options)).final()
    }

    return (await this.db.getBlock(this.gte++, this.options)).final()
  }
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
