const b4a = require('b4a')

module.exports = class RangeIterator {
  constructor(batch, encoding, opts = {}) {
    this.batch = batch
    this.stack = []
    this.opened = false
    this.encoding = encoding || batch.encoding

    this._limit = typeof opts.limit === 'number' ? opts.limit : -1
    this._gIncl = !opts.gt
    this._gKey = opts.gt || opts.gte || null
    this._lIncl = !opts.lt
    this._lKey = opts.lt || opts.lte || null
    this._reverse = !!opts.reverse
    this._version = 0
    this._checkpoint = opts.checkpoint && opts.checkpoint.length ? opts.checkpoint : null
    this._nexting = false
    this._closed = false
    // Issue all reads of a node at once so storage serves them in one batch
    this._eager = this._limit === -1 || this._limit > 1
  }

  snapshot(version = this.batch.version) {
    const checkpoint = []
    for (const s of this.stack) {
      let { node, i } = s
      if (this._nexting && s === this.stack[this.stack.length - 1]) {
        i = this._reverse ? i + 1 : i - 1
      }
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

  async open() {
    await this._open()
    this.opened = true
  }

  async _open() {
    if (this._checkpoint) {
      for (let j = 0; j < this._checkpoint.length; j += 3) {
        const seq = this._checkpoint[j]
        const offset = this._checkpoint[j + 1]
        const i = this._checkpoint[j + 2]
        this._push((await this.batch.getBlock(seq)).getTreeNode(offset), i)
      }
      return
    }

    this._nexting = true

    let node = await this.batch.getRoot(false)
    if (!node) {
      this._nexting = false
      return
    }

    const incl = this._reverse ? this._lIncl : this._gIncl
    const start = this._reverse ? this._lKey : this._gKey

    if (!start) {
      this._push(node, this._reverse ? node.keys.length << 1 : 0)
      this._nexting = false
      this._preloadQuery()
      return
    }

    while (true) {
      const entry = new Frame(node, this._reverse ? node.keys.length << 1 : 0)

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(start, await node.getKey(mid))

        if (c === 0) {
          if (incl) entry.i = mid * 2 + 1
          else entry.i = mid * 2 + (this._reverse ? 0 : 2)
          this._pushFrame(entry)
          this._nexting = false
          this._preloadQuery()
          return
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      entry.i = 2 * i + (this._reverse ? -1 : 1)

      if (entry.i >= 0 && entry.i <= node.keys.length << 1) this._pushFrame(entry)
      if (!node.children.length) {
        this._nexting = false
        this._preloadQuery()
        return
      }

      node = await node.getChildNode(i)
    }
  }

  _push(node, i) {
    this._pushFrame(new Frame(node, i))
  }

  _pushFrame(frame) {
    if (this._eager) frame.load(this.batch, this._reverse)
    this.stack.push(frame)
  }

  _preloadQuery() {
    const max = { nodes: 0, max: 2048 }

    if (this._limit === -1 && !this._reverse && this.stack.length) {
      for (const s of this.stack) {
        const k = (s.i - (s.i & 1)) / 2
        this._preload(s.node, k + 1, this._lKey, max).catch(noop)
      }
    }
  }

  async _preload(node, i, end, max) {
    for (; i < node.keys.length; i++) {
      const key = node.keys[i]
      const block = await this.batch.getBlock(key.seq)
      if (this._closed) return
      const c = end ? b4a.compare(block.key, end) : -1
      if (c >= 0 || max.nodes >= max.max || i >= node.children.length) return
      max.nodes++
      const next = await node.getChildNode(i)
      if (this._closed) return
      this._preload(next, 0, end, max).catch(noop)
    }

    if (node.children.length && max.nodes < max.max) {
      const next = await node.getChildNode(node.children.length - 1)
      if (this._closed) return
      this._preload(next, 0, end, max).catch(noop)
    }
  }

  async next() {
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
      const n = this._reverse ? (top.i < 0 ? top.node.keys.length : top.i-- >> 1) : top.i++ >> 1

      if (!isKey) {
        if (!top.node.children.length) continue
        const child = await top.child(n)
        if (top.node.block.seq < this.batch.core.length) {
          top.node.children[n].value = null // unlink it to save memory
        }
        if (child instanceof Frame) {
          if (child.blocks === null) child.load(this.batch, this._reverse)
          this.stack.push(child)
        } else {
          this._push(child, this._reverse ? child.keys.length << 1 : 0)
        }
        continue
      }

      if (n >= top.node.keys.length) {
        this.stack.pop()
        continue
      }

      const block = await top.block(this.batch, n)
      if (end) {
        const c = b4a.compare(block.key, end)
        if (c === 0 ? !incl : this._reverse ? c < 0 : c > 0) {
          this._limit = 0
          break
        }
      }
      if (this._limit > 0) this._limit--
      this._nexting = false
      return block.final(this.encoding)
    }

    this._nexting = false
    return null
  }

  close() {
    this._closed = true
    return this.batch._closeSnapshot()
  }
}

class Frame {
  constructor(node, i) {
    this.node = node
    this.i = i
    this.blocks = null
    this.children = null
  }

  // start the reads for every key and child still ahead of i, without awaiting
  load(batch, reverse) {
    const node = this.node
    const n = this.i >> 1
    const keys = node.keys.length
    const children = node.children.length

    this.blocks = new Array(keys)
    for (let j = reverse ? 0 : n; j < (reverse ? Math.min(n + 1, keys) : keys); j++) {
      const p = batch.getBlock(node.keys[j].seq)
      p.catch(noop)
      this.blocks[j] = p
    }

    if (children === 0) return

    this.children = new Array(children)
    for (let j = reverse ? 0 : n; j < (reverse ? Math.min(n + 1, children) : children); j++) {
      const p = node.getChildNode(j).then((child) => childFrame(batch, child, reverse))
      p.catch(noop)
      this.children[j] = p
    }
  }

  block(batch, n) {
    const p = this.blocks === null ? undefined : this.blocks[n]
    if (p === undefined) return batch.getBlock(this.node.keys[n].seq)
    this.blocks[n] = undefined
    return p
  }

  child(n) {
    const p = this.children === null ? undefined : this.children[n]
    if (p === undefined) return this.node.getChildNode(n)
    this.children[n] = undefined
    return p
  }
}

// leaves start their key reads as soon as they arrive, inner nodes wait until visited
function childFrame(batch, node, reverse) {
  const frame = new Frame(node, reverse ? node.keys.length << 1 : 0)
  if (node.children.length === 0) frame.load(batch, reverse)
  return frame
}

function noop() {}
