module.exports = class HistoryIterator {
  constructor (db, opts = {}) {
    this.db = db
    this.options = opts
    this.live = !!opts.live
    this.since = opts.since || 1
    this.end = 0
  }

  async open () {
    await this.db.ready()
    this.end = this.live ? Infinity : this.db.version
  }

  async next () {
    if (this.since >= this.end) return null
    return this.db.getBlock(this.since++, this.options)
  }
}
