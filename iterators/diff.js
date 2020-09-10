class SubTree {
  constructor (node, parent) {
    this.node = node
    this.parent = parent

    this.isKey = node.children.length === 0
    this.i = this.isKey ? 1 : 0
    this.n = 0

    const child = this.isKey ? null : this.node.children[0]
    this.seq = child !== null ? child.seq : this.node.keys[0].seq
    this.offset = child !== null ? child.offset : 0
  }

  next () {
    this.i++
    this.isKey = (this.i & 1) === 1
    if (!this.isKey && !this.node.children.length) this.i++
    return this.update()
  }

  async bisect (key, incl) {
    let s = 0
    let e = this.node.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = cmp(key, await this.node.getKey(mid))

      if (c === 0) {
        if (incl) this.i = mid * 2 + 1
        else this.i = mid * 2 + (this.node.children.length ? 2 : 3)
        return true
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.i = 2 * i + (this.node.children.length ? 0 : 1)
    return this.node.children.length === 0
  }

  update () {
    this.isKey = (this.i & 1) === 1
    this.n = this.i >> 1
    if (this.n >= (this.isKey ? this.node.keys.length : this.node.children.length)) return false
    const child = this.isKey ? null : this.node.children[this.n]
    this.seq = child !== null ? child.seq : this.node.keys[this.n].seq
    this.offset = child !== null ? child.offset : 0
    return true
  }

  async key () {
    return this.n < this.node.keys.length ? this.node.getKey(this.n) : (this.parent && this.parent.key())
  }

  async compare (tree) {
    const [a, b] = await Promise.all([this.key(), tree.key()])
    return cmp(a, b)
  }
}

class TreeIterator {
  constructor (db, opts) {
    this.db = db
    this.stack = []
    this.lt = opts.lt || opts.lte || null
    this.lte = !!opts.lte
    this.gt = opts.gt || opts.gte || null
    this.gte = !!opts.gte
    this.seeking = !!this.gt
  }

  async open () {
    const node = await this.db.getRoot()
    if (!node.keys.length) return
    const tree = new SubTree(node, null)
    if (this.seeking && !(await this._seek(tree))) return
    this.stack.push(tree)
  }

  async _seek (tree) {
    const done = await tree.bisect(this.gt, this.gte)
    const oob = !tree.update()
    if (done || oob) {
      this.seeking = false
      if (oob) return false
    }
    return true
  }

  peek () {
    if (!this.stack.length) return null
    return this.stack[this.stack.length - 1]
  }

  skip () {
    if (!this.stack.length) return
    if (!this.stack[this.stack.length - 1].next()) this.stack.pop()
  }

  async nextKey () {
    let n = null
    while (this.stack.length && n === null) n = await this.next()
    if (!this.lt) return n.final()

    const c = cmp(n.key, this.lt)
    if (this.lte ? c <= 0 : c < 0) return n.final()
    this.stack = []
    return null
  }

  async next () {
    if (!this.stack.length) return null

    const top = this.stack[this.stack.length - 1]
    const { isKey, n, seq } = top

    if (!top.next()) {
      this.stack.pop()
    }

    if (isKey) {
      this.seeking = false
      return this.db.getBlock(seq)
    }

    const child = await top.node.getChildNode(n)
    top.node.children[n] = null // unlink to save memory
    const tree = new SubTree(child, top)
    if (this.seeking && !(await this._seek(tree))) return
    this.stack.push(tree)

    return null
  }
}

module.exports = class DiffIterator {
  constructor (left, right, opts = {}) {
    this.left = new TreeIterator(left, opts)
    this.right = new TreeIterator(right, opts)
    this.limit = typeof opts.limit === 'number' ? opts.limit : -1
  }

  async open () {
    await Promise.all([this.left.open(), this.right.open()])
  }

  async next () {
    if (this.limit === 0) return null
    const res = await this._next()
    if (!res || (res.left === null && res.right === null)) return null
    this.limit--
    return res
  }

  async _next () {
    const a = this.left
    const b = this.right

    while (true) {
      const [l, r] = await Promise.all([a.peek(), b.peek()])

      if (!l && !r) return null
      if (!l) return { left: null, right: await b.nextKey() }
      if (!r) return { left: await a.nextKey(), right: null }

      if (l.seq === r.seq && l.isKey === r.isKey && l.offset === r.offset) {
        a.skip()
        b.skip()
        continue
      }

      const c = await l.compare(r)

      if (l.isKey && !r.isKey) {
        if (c > 0) b.skip()
        else await b.next()
        continue
      }

      if (!l.isKey && r.isKey) {
        if (c < 0) a.skip()
        else await a.next()
        continue
      }

      if (l.isKey && r.isKey) {
        if (c === 0) return { left: await a.nextKey(), right: await b.nextKey() }
        if (c < 0) return { left: await a.nextKey(), right: null }
        return { left: null, right: await b.nextKey() }
      }

      if (c === 0) await Promise.all([a.next(), b.next()])
      else if (c < 0) await b.next()
      else await a.next()
    }
  }
}

function cmp (a, b) {
  if (!a) return b ? 1 : 0
  if (!b) return a ? -1 : 0
  return a < b ? -1 : b < a ? 1 : 0
}
