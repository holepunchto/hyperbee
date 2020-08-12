module.exports = class HistoryIterator {
  constructor (db, opts = {}) {
    this.db = db
    this.options = opts
    this.live = !!opts.live
    this.since = opts.since || 0
    this.end = 0
    this.reverse = !!opts.reverse
    this.limit = typeof opts.limit === 'number' ? opts.limit : -1
  }

  async open () {
    await this.db.getRoot() // does the update dance
    this.end = this.live ? Infinity : this.db.version
    if (this.since) return
    if (this.reverse) this.since = this.end - 1
    else this.since = 1
  }

  async next () {
    if (this._limit === 0) return null
    if (this._limit > 0) this._limit--

    if (this.reverse) {
      if (this.since < 1) return null
      return (await this.db.getBlock(this.since--, this.options)).final()
    }

    if (this.since >= this.end) return null
    return (await this.db.getBlock(this.since++, this.options)).final()
  }
}
