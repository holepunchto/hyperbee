module.exports = class RangeIterator {
  constructor (db, opts = {}) {
    this.db = db
    this.stack = []
    this.opened = false

    this._limit = typeof opts.limit === 'number' ? opts.limit : -1
    this._gIncl = !opts.gt
    this._gKey = opts.gt || opts.gte || null
    this._lIncl = !opts.lt
    this._lKey = opts.lt || opts.lte || null
    this._reverse = !!opts.reverse
    this._version = 0
    this._checkpoint = (opts.checkpoint && opts.checkpoint.length) ? opts.checkpoint : null
    this._nexting = false
  }

  snapshot (version = this.db.version) {
    const checkpoint = []
    for (const s of this.stack) {
      let { node, i } = s
      if (this._nexting && s === this.stack[this.stack.length - 1]) i = this._reverse ? i + 1 : i - 1
      if (!node.block) continue
      if (i < 0) continue
      checkpoint.push(node.block.seq, node.offset, i)
    }

    return {
      version,
      gte: this._gIncl ? this._gKey : null,
      gt: this._gIncl ? null : this._gKey,
      lte: this._lIncl ? this._lKey : null,
      lt: this._lIncl ? null : this._lKey,
      limit: this._limit,
      reverse: this._reverse,
      ended: this.opened && !checkpoint.length,
      checkpoint: this.opened ? checkpoint : []
    }
  }

  async open () {
    await this._open()
    this.opened = true
  }

  async _open () {
    if (this._checkpoint) {
      for (let j = 0; j < this._checkpoint.length; j += 3) {
        const seq = this._checkpoint[j]
        const offset = this._checkpoint[j + 1]
        const i = this._checkpoint[j + 2]
        this.stack.push({
          node: (await this.db.getBlock(seq)).getTreeNode(offset),
          i
        })
      }
      return
    }

    this._nexting = true

    let node = await this.db.getRoot(false)
    if (!node) {
      this._nexting = false
      return
    }

    const incl = this._reverse ? this._lIncl : this._gIncl
    const start = this._reverse ? this._lKey : this._gKey

    if (!start) {
      this.stack.push({ node, i: this._reverse ? node.keys.length << 1 : 0 })
      this._nexting = false
      return
    }

    while (true) {
      const entry = { node, i: this._reverse ? node.keys.length << 1 : 0 }

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = Buffer.compare(start, await node.getKey(mid))

        if (c === 0) {
          if (incl) entry.i = mid * 2 + 1
          else entry.i = mid * 2 + (this._reverse ? 0 : 2)
          this.stack.push(entry)
          this._nexting = false
          return
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      entry.i = 2 * i + (this._reverse ? -1 : 1)

      if (entry.i >= 0 && entry.i <= (node.keys.length << 1)) this.stack.push(entry)
      if (!node.children.length) {
        this._nexting = false
        return
      }

      node = await node.getChildNode(i)
    }
  }

  async next () {
    // TODO: this nexting flag is only needed if someone asks for a snapshot during
    // a lookup (ie the extension, pretty important...).
    // A better solution would be to refactor this so top.i is incremented eagerly
    // to get the current block instead of the way it is done now (++i vs i++)
    this._nexting = true

    const end = this._reverse ? this._gKey : this._lKey
    const incl = this._reverse ? this._gIncl : this._lIncl

    while (this.stack.length && (this._limit === -1 || this._limit > 0)) {
      const top = this.stack[this.stack.length - 1]
      const isKey = (top.i & 1) === 1
      const n = this._reverse
        ? (top.i < 0 ? top.node.keys.length : top.i-- >> 1)
        : top.i++ >> 1

      if (!isKey) {
        if (!top.node.children.length) continue
        const node = await top.node.getChildNode(n)
        top.node.children[n] = null // unlink it to save memory
        this.stack.push({ i: this._reverse ? node.keys.length << 1 : 0, node })
        continue
      }

      if (n >= top.node.keys.length) {
        this.stack.pop()
        continue
      }

      const key = top.node.keys[n]
      const block = await this.db.getBlock(key.seq)
      if (end) {
        const c = Buffer.compare(block.key, end)
        if (c === 0 ? !incl : (this._reverse ? c < 0 : c > 0)) {
          this._limit = 0
          break
        }
      }
      if (this._limit > 0) this._limit--
      this._nexting = false
      return block.final()
    }

    this._nexting = false
    return null
  }
}
