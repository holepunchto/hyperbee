const codecs = require('codecs')
const { Readable } = require('streamx')
const mutexify = require('mutexify/promise')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')

const RangeIterator = require('./iterators/range')
const HistoryIterator = require('./iterators/history')
const DiffIterator = require('./iterators/diff')
const Extension = require('./lib/extension')
const { YoloIndex, Node, Header } = require('./lib/messages')

const T = 5
const MIN_KEYS = T - 1
const MAX_CHILDREN = MIN_KEYS * 2 + 1

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

class Key {
  constructor (seq, value) {
    this.seq = seq
    this.value = value
  }
}

class Child {
  constructor (seq, offset, value) {
    this.seq = seq
    this.offset = offset
    this.value = value
  }
}

class Pointers {
  constructor (buf) {
    this.levels = YoloIndex.decode(buf).levels.map(l => {
      const children = []
      const keys = []

      for (let i = 0; i < l.keys.length; i++) {
        keys.push(new Key(l.keys[i], null))
      }

      for (let i = 0; i < l.children.length; i += 2) {
        children.push(new Child(l.children[i], l.children[i + 1], null))
      }

      return { keys, children }
    })
  }

  get (i) {
    return this.levels[i]
  }

  hasKey (seq) {
    for (const lvl of this.levels) {
      for (const key of lvl.keys) {
        if (key.seq === seq) return true
      }
    }
    return false
  }
}

function inflate (buf) {
  return new Pointers(buf)
}

function deflate (index) {
  const levels = index.map(l => {
    const keys = []
    const children = []

    for (let i = 0; i < l.value.keys.length; i++) {
      keys.push(l.value.keys[i].seq)
    }

    for (let i = 0; i < l.value.children.length; i++) {
      children.push(l.value.children[i].seq, l.value.children[i].offset)
    }

    return { keys, children }
  })

  return YoloIndex.encode({ levels })
}

class TreeNode {
  constructor (block, keys, children, offset) {
    this.block = block
    this.offset = offset
    this.keys = keys
    this.children = children
    this.changed = false
  }

  async insertKey (key, child, node, encoding, cas) {
    let s = 0
    let e = this.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = b4a.compare(key.value, await this.getKey(mid))

      if (c === 0) {
        if (cas && !(await cas((await this.getKeyNode(mid)).final(encoding), node))) return true
        this.changed = true
        this.keys[mid] = key
        return true
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.splice(i, 0, key)
    if (child) this.children.splice(i + 1, 0, new Child(0, 0, child))
    this.changed = true

    return this.keys.length < MAX_CHILDREN
  }

  removeKey (index) {
    this.keys.splice(index, 1)
    if (this.children.length) {
      this.children[index + 1].seq = 0 // mark as freed
      this.children.splice(index + 1, 1)
    }
    this.changed = true
  }

  async siblings (parent) {
    for (let i = 0; i < parent.children.length; i++) {
      if (parent.children[i].value === this) {
        const left = i ? parent.getChildNode(i - 1) : null
        const right = i < parent.children.length - 1 ? parent.getChildNode(i + 1) : null
        return { left: await left, index: i, right: await right }
      }
    }

    throw new Error('Bad parent')
  }

  merge (node, median) {
    this.changed = true
    this.keys.push(median)
    for (let i = 0; i < node.keys.length; i++) this.keys.push(node.keys[i])
    for (let i = 0; i < node.children.length; i++) this.children.push(node.children[i])
  }

  async split () {
    const len = this.keys.length >> 1
    const right = TreeNode.create(this.block)

    while (right.keys.length < len) right.keys.push(this.keys.pop())
    right.keys.reverse()

    await this.getKey(this.keys.length - 1) // make sure the median is loaded
    const median = this.keys.pop()

    if (this.children.length) {
      while (right.children.length < len + 1) right.children.push(this.children.pop())
      right.children.reverse()
    }

    this.changed = true

    return {
      left: this,
      median,
      right
    }
  }

  getKeyNode (index) {
    return this.block.tree.getBlock(this.keys[index].seq)
  }

  async getChildNode (index) {
    const child = this.children[index]
    if (child.value) return child.value
    const block = child.seq === this.block.seq ? this.block : await this.block.tree.getBlock(child.seq)
    return (child.value = block.getTreeNode(child.offset))
  }

  setKey (index, key) {
    this.keys[index] = key
    this.changed = true
  }

  async getKey (index) {
    const key = this.keys[index]
    if (key.value) return key.value
    const k = key.seq === this.block.seq ? this.block.key : await this.block.tree.getKey(key.seq)
    return (key.value = k)
  }

  indexChanges (index, seq) {
    const offset = index.push(null) - 1
    this.changed = false

    for (const child of this.children) {
      if (!child.value || !child.value.changed) continue
      child.seq = seq
      child.offset = child.value.indexChanges(index, seq)
      index[child.offset] = child
    }

    return offset
  }

  static create (block) {
    const node = new TreeNode(block, [], [], 0)
    node.changed = true
    return node
  }
}

class BlockEntry {
  constructor (seq, tree, entry) {
    this.seq = seq
    this.tree = tree
    this.index = null
    this.indexBuffer = entry.index
    this.key = entry.key
    this.value = entry.value
  }

  isDeletion () {
    if (this.value !== null) return false

    if (this.index === null) {
      this.index = inflate(this.indexBuffer)
      this.indexBuffer = null
    }

    return !this.index.hasKey(this.seq)
  }

  final (encoding) {
    return {
      seq: this.seq,
      key: encoding.key ? encoding.key.decode(this.key) : this.key,
      value: this.value && (encoding.value ? encoding.value.decode(this.value) : this.value)
    }
  }

  getTreeNode (offset) {
    if (this.index === null) {
      this.index = inflate(this.indexBuffer)
      this.indexBuffer = null
    }
    const entry = this.index.get(offset)
    return new TreeNode(this, entry.keys, entry.children, offset)
  }
}

class BatchEntry extends BlockEntry {
  constructor (seq, tree, key, value, index) {
    super(seq, tree, { key, value, index: null })
    this.pendingIndex = index
  }

  getTreeNode (offset) {
    return this.pendingIndex[offset].value
  }
}

class Hyperbee {
  constructor (feed, opts = {}) {
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = feed
    this.core = feed

    this.keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : null
    this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
    this.extension = opts.extension !== false ? opts.extension || Extension.register(this) : null
    this.metadata = opts.metadata || null
    this.lock = opts.lock || mutexify()
    this.sep = opts.sep || SEP
    this.readonly = !!opts.readonly
    this.prefix = opts.prefix || null

    this._unprefixedKeyEncoding = this.keyEncoding
    this._sub = !!this.prefix
    this._checkout = opts.checkout || 0
    this._ready = opts._ready || null

    this._watchers = new Set()
    this._onappendBound = this._onappend.bind(this)
    this.core.on('append', this._onappendBound)
    if (this.core.isAutobase) this.core.on('truncate', this._onappendBound)

    if (this.prefix && opts._sub) {
      this.keyEncoding = prefixEncoding(this.prefix, this.keyEncoding)
    }
  }

  ready () {
    return this.feed.ready()
  }

  get version () {
    return Math.max(1, this._checkout || this.feed.length)
  }

  update () {
    return this.feed.update({ ifAvailable: true, hash: false })
  }

  peek (opts) {
    return iteratorPeek(this.createRangeIterator({ ...opts, limit: 1 }))
  }

  createRangeIterator (opts = {}) {
    const extension = (opts.extension === false && opts.limit !== 0) ? null : this.extension
    const keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : this.keyEncoding

    if (extension) {
      const { onseq, onwait } = opts
      let version = 0
      let next = 0

      opts = encRange(keyEncoding, {
        ...opts,
        sub: this._sub,
        onseq (seq) {
          if (!version) version = seq + 1
          if (next) next--
          if (onseq) onseq(seq)
        },
        onwait (seq) {
          if (!next) {
            next = Extension.BATCH_SIZE
            extension.iterator(ite.snapshot(version))
          }
          if (onwait) onwait(seq)
        }
      })
    } else {
      opts = encRange(keyEncoding, { ...opts, sub: this._sub })
    }

    const ite = new RangeIterator(new Batch(this, this._makeSnapshot(), null, false, opts), null, opts)
    return ite
  }

  createReadStream (opts) {
    return iteratorToStream(this.createRangeIterator(opts))
  }

  createHistoryStream (opts) {
    const session = (opts && opts.live) ? this.feed.session() : this._makeSnapshot()
    return iteratorToStream(new HistoryIterator(new Batch(this, session, null, false, opts), opts))
  }

  createDiffStream (right, opts) {
    if (typeof right === 'number') right = this.checkout(Math.max(1, right))
    const snapshot = right.version > this.version ? right._makeSnapshot() : this._makeSnapshot()

    const keyEncoding = opts && opts.keyEncoding ? codecs(opts.keyEncoding) : this.keyEncoding
    if (keyEncoding) opts = encRange(keyEncoding, { ...opts, sub: this._sub })

    return iteratorToStream(new DiffIterator(new Batch(this, snapshot, null, false, opts), new Batch(right, snapshot, null, false, opts), opts))
  }

  get (key, opts) {
    const b = new Batch(this, this._makeSnapshot(), null, true, opts)
    return b.get(key)
  }

  put (key, value, opts) {
    const b = new Batch(this, this.feed, null, true, opts)
    return b.put(key, value, opts)
  }

  batch (opts) {
    return new Batch(this, this.feed, mutexify(), true, opts)
  }

  del (key, opts) {
    const b = new Batch(this, this.feed, null, true, opts)
    return b.del(key, opts)
  }

  watch (range, opts) {
    return new Watcher(this, range, opts)
  }

  _onappend () {
    for (const watcher of this._watchers) {
      watcher._onappend()
    }
  }

  _makeSnapshot () {
    // TODO: better if we could encapsulate this in hypercore in the future
    return this._checkout <= this.feed.length ? this.feed.snapshot() : this.feed.session()
  }

  checkout (version, opts = {}) {
    // same as above, just checkout isn't set yet...
    const snap = version <= this.feed.length ? this.feed.snapshot() : this.feed.session()

    return new Hyperbee(snap, {
      _ready: this.ready(),
      _sub: false,
      sep: this.sep,
      prefix: this.prefix,
      checkout: version,
      keyEncoding: opts.keyEncoding || this.keyEncoding,
      valueEncoding: opts.valueEncoding || this.valueEncoding,
      extension: this.extension !== null ? this.extension : false
    })
  }

  snapshot (opts) {
    return this.checkout(this.version, opts)
  }

  sub (prefix, opts = {}) {
    let sep = opts.sep || this.sep
    if (!b4a.isBuffer(sep)) sep = b4a.from(sep)

    prefix = b4a.concat([this.prefix || EMPTY, b4a.from(prefix), sep])

    const valueEncoding = codecs(opts.valueEncoding || this.valueEncoding)
    const keyEncoding = codecs(opts.keyEncoding || this._unprefixedKeyEncoding)

    return new Hyperbee(this.feed, {
      _ready: this.ready(),
      _sub: true,
      prefix,
      sep: this.sep,
      lock: this.lock,
      checkout: this._checkout,
      valueEncoding,
      keyEncoding,
      extension: this.extension !== null ? this.extension : false,
      metadata: this.metadata
    })
  }

  async getHeader (opts) {
    const blk = await this.feed.get(0, opts)
    return blk && Header.decode(blk)
  }

  async close () {
    this.core.off('append', this._onappendBound)
    if (this.core.isAutobase) this.core.off('truncate', this._onappendBound)

    for (const watcher of this._watchers) {
      await watcher.destroy()
    }

    return this.feed.close()
  }

  static async isHyperbee (core, opts) {
    await core.ready()

    const blk0 = await core.get(0, opts)
    if (blk0 === null) throw new Error('Block 0 not available locally')

    try {
      return Header.decode(blk0).protocol === 'hyperbee'
    } catch (err) { // undecodable
      safetyCatch(err)
      return false
    }
  }
}

class Batch {
  constructor (tree, feed, batchLock, cache, options = {}) {
    this.tree = tree
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = feed
    this.core = feed
    this.blocks = cache ? new Map() : null
    this.autoFlush = !batchLock
    this.rootSeq = 0
    this.root = null
    this.length = 0
    this.options = options
    this.locked = null
    this.batchLock = batchLock
    this.onseq = this.options.onseq || noop
    this.appending = null
    this.isSnapshot = this.feed !== this.tree.feed
    this.shouldUpdate = this.options.update !== false
    this.encoding = {
      key: options.keyEncoding ? codecs(options.keyEncoding) : tree.keyEncoding,
      value: options.valueEncoding ? codecs(options.valueEncoding) : tree.valueEncoding
    }
  }

  ready () {
    return this.tree.ready()
  }

  async lock () {
    if (this.tree.readonly) throw new Error('Hyperbee is marked as read-only')
    if (this.locked === null) this.locked = await this.tree.lock()
  }

  get version () {
    return Math.max(1, this.tree._checkout ? this.tree._checkout : this.feed.length + this.length)
  }

  async getRoot (ensureHeader) {
    await this.ready()
    if (ensureHeader) {
      if (this.feed.length === 0 && this.feed.writable && !this.tree.readonly) {
        await this.feed.append(Header.encode({
          protocol: 'hyperbee',
          metadata: this.tree.metadata
        }))
      }
    }
    if (this.tree._checkout === 0 && this.shouldUpdate) await this.feed.update()
    if (this.version < 2) return null
    return (await this.getBlock(this.version - 1)).getTreeNode(0)
  }

  async getKey (seq) {
    return (await this.getBlock(seq)).key
  }

  async getBlock (seq) {
    if (this.rootSeq === 0) this.rootSeq = seq
    let b = this.blocks && this.blocks.get(seq)
    if (b) return b
    this.onseq(seq)
    const entry = await this.feed.get(seq, { ...this.options, valueEncoding: Node })
    if (entry === null) throw new Error('Block not available locally')
    b = new BlockEntry(seq, this, entry)
    if (this.blocks && (this.blocks.size - this.length) < 128) this.blocks.set(seq, b)
    return b
  }

  _onwait (key) {
    this.options.onwait = null
    this.tree.extension.get(this.rootSeq, key)
  }

  _getEncoding (opts) {
    if (!opts) return this.encoding
    return {
      key: opts.keyEncoding ? codecs(opts.keyEncoding) : this.encoding.key,
      value: opts.valueEncoding ? codecs(opts.valueEncoding) : this.encoding.value
    }
  }

  peek (opts) {
    return iteratorPeek(this.createRangeIterator({ ...opts, limit: 1 }))
  }

  createRangeIterator (opts = {}) {
    const encoding = this._getEncoding(opts)
    return new RangeIterator(this, encoding, encRange(encoding.key, { ...opts, sub: this.tree._sub }))
  }

  createReadStream (opts) {
    return iteratorToStream(this.createRangeIterator(opts))
  }

  async get (key, opts) {
    const encoding = this._getEncoding(opts)

    try {
      return await this._get(key, encoding)
    } finally {
      await this._closeSnapshot()
    }
  }

  async _get (key, encoding) {
    key = enc(encoding.key, key)

    if (this.tree.extension !== null && this.options.extension !== false) {
      this.options.onwait = this._onwait.bind(this, key)
    }

    let node = await this.getRoot(false)
    if (!node) return null

    while (true) {
      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1

        c = b4a.compare(key, await node.getKey(mid))

        if (c === 0) return (await this.getBlock(node.keys[mid].seq)).final(encoding)

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.children.length) return null

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }
  }

  async put (key, value, opts) {
    const release = this.batchLock ? await this.batchLock() : null

    const cas = (opts && opts.cas) || null
    const encoding = this._getEncoding(opts)

    if (!this.locked) await this.lock()
    if (!release) return this._put(key, value, encoding, cas)

    try {
      return await this._put(key, value, encoding, cas)
    } finally {
      release()
    }
  }

  async _put (key, value, encoding, cas) {
    const newNode = {
      seq: 0,
      key,
      value
    }
    key = enc(encoding.key, key)
    value = enc(encoding.value, value)

    const stack = []

    let root
    let node = root = await this.getRoot(true)
    if (!node) node = root = TreeNode.create(null)

    const seq = newNode.seq = this.feed.length + this.length
    const target = new Key(seq, key)

    while (node.children.length) {
      stack.push(node)
      node.changed = true // changed, but compressible

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(target.value, await node.getKey(mid))

        if (c === 0) {
          if (cas && !(await cas((await node.getKeyNode(mid)).final(encoding), newNode))) return this._unlockMaybe()

          node.setKey(mid, target)
          return this._append(root, seq, key, value)
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }

    let needsSplit = !(await node.insertKey(target, null, newNode, encoding, cas))
    if (!node.changed) return this._unlockMaybe()

    while (needsSplit) {
      const parent = stack.pop()
      const { median, right } = await node.split()

      if (parent) {
        needsSplit = !(await parent.insertKey(median, right, null, encoding, null))
        node = parent
      } else {
        root = TreeNode.create(node.block)
        root.changed = true
        root.keys.push(median)
        root.children.push(new Child(0, 0, node), new Child(0, 0, right))
        needsSplit = false
      }
    }

    return this._append(root, seq, key, value)
  }

  async del (key, opts) {
    const release = this.batchLock ? await this.batchLock() : null
    const cas = (opts && opts.cas) || null
    const encoding = this._getEncoding(opts)

    if (!this.locked) await this.lock()
    if (!release) return this._del(key, encoding, cas)

    try {
      return await this._del(key, encoding, cas)
    } finally {
      release()
    }
  }

  async _del (key, encoding, cas) {
    const delNode = {
      seq: 0,
      key,
      value: null
    }

    key = enc(encoding.key, key)

    const stack = []

    let node = await this.getRoot(true)
    if (!node) return this._unlockMaybe()

    const seq = delNode.seq = this.feed.length + this.length

    while (true) {
      stack.push(node)

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(key, await node.getKey(mid))

        if (c === 0) {
          if (cas && !(await cas((await node.getKeyNode(mid)).final(encoding), delNode))) return this._unlockMaybe()
          if (node.children.length) await setKeyToNearestLeaf(node, mid, stack)
          else node.removeKey(mid)
          // we mark these as changed late, so we don't rewrite them if it is a 404
          for (const node of stack) node.changed = true
          return this._append(await rebalance(stack), seq, key, null)
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.children.length) return this._unlockMaybe()

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }
  }

  async _closeSnapshot () {
    if (this.isSnapshot) await this.feed.close()
  }

  async close () {
    if (this.isSnapshot) {
      await this.feed.close()
      return
    }

    this.root = null
    this.blocks.clear()
    this.length = 0
    this._unlock()
  }

  destroy () { // compat, remove later
    this.close().catch(noop)
  }

  toBlocks () {
    if (this.appending) return this.appending

    const batch = new Array(this.length)

    for (let i = 0; i < this.length; i++) {
      const seq = this.feed.length + i
      const { pendingIndex, key, value } = this.blocks.get(seq)

      if (i < this.length - 1) {
        pendingIndex[0] = null
        let j = 0

        while (j < pendingIndex.length) {
          const idx = pendingIndex[j]
          if (idx !== null && idx.seq === seq) {
            idx.offset = j++
            continue
          }
          if (j === pendingIndex.length - 1) pendingIndex.pop()
          else pendingIndex[j] = pendingIndex.pop()
        }
      }

      batch[i] = Node.encode({
        key,
        value,
        index: deflate(pendingIndex)
      })
    }

    this.appending = batch
    return batch
  }

  flush () {
    if (!this.length) return this.close()

    const batch = this.toBlocks()

    this.root = null
    this.blocks.clear()
    this.length = 0

    return this._appendBatch(batch)
  }

  _unlockMaybe () {
    if (this.autoFlush) this._unlock()
  }

  _unlock () {
    const locked = this.locked
    this.locked = null
    if (locked !== null) locked()
  }

  _append (root, seq, key, value) {
    const index = []
    root.indexChanges(index, seq)
    index[0] = new Child(seq, 0, root)

    if (!this.autoFlush) {
      const block = new BatchEntry(seq, this, key, value, index)
      if (!root.block) root.block = block
      this.root = root
      this.length++
      this.blocks.set(seq, block)
      return
    }

    return this._appendBatch(Node.encode({
      key,
      value,
      index: deflate(index)
    }))
  }

  async _appendBatch (raw) {
    try {
      await this.feed.append(raw)
    } finally {
      this._unlock()
    }
  }
}

class Watcher {
  constructor (bee, range, opts = {}) {
    bee._watchers.add(this)

    this.bee = bee
    this.core = bee.core

    this.opened = false
    this.closed = false

    this.latestDiff = 0
    this.range = range
    this.map = opts.map || defaultWatchMap

    this.current = null
    this.previous = null
    this.stream = null

    this._lock = mutexify()
    this._resolveOnChange = null

    this._closing = null
    this._opening = this._ready()
    this._opening.catch(safetyCatch)

    this._differ = opts.differ || defaultDiffer
  }

  ready () {
    return this._opening
  }

  async _ready () {
    await this.bee.ready()
    this.current = this.bee.snapshot() // Point from which to start watching
    this.opened = true
  }

  [Symbol.asyncIterator] () {
    return this
  }

  _onappend () {
    const resolve = this._resolveOnChange
    this._resolveOnChange = null
    if (resolve) resolve()
  }

  async _waitForChanges () {
    if (this.current.version < this.bee.version || this.closed) return

    await new Promise(resolve => {
      this._resolveOnChange = resolve
    })
  }

  async next () {
    try {
      return await this._next()
    } catch (err) {
      if (this.closed) return { value: undefined, done: true }
      await this.destroy()
      throw err
    }
  }

  async _next () {
    const release = await this._lock()

    try {
      if (this.closed) return { value: undefined, done: true }

      if (!this.opened) await this._opening

      while (true) {
        await this._waitForChanges()

        if (this.closed) return { value: undefined, done: true }

        if (this.previous) await this.previous.close()
        this.previous = this.current.snapshot()

        if (this.current) await this.current.close()
        this.current = this.bee.snapshot()

        this.stream = this._differ(this.current, this.previous, this.range)

        try {
          for await (const data of this.stream) { // eslint-disable-line
            return { done: false, value: [this.map(this.current), this.map(this.previous)] }
          }
        } finally {
          this.stream = null
        }
      }
    } finally {
      release()
    }
  }

  async return () {
    await this.destroy()
    return { done: true }
  }

  async destroy () {
    if (this._closing) return this._closing
    this._closing = this._destroy()
    return this._closing
  }

  async _destroy () {
    if (this.closed) return
    this.closed = true

    if (!this.opened) await this._opening.catch(safetyCatch)

    this.bee._watchers.delete(this)

    if (this.stream && !this.stream.destroying) {
      this.stream.destroy()
    }

    this._onappend() // Continue execution being closed

    await this._closeSnapshots()

    const release = await this._lock()
    release()
  }

  _closeSnapshots () {
    const closing = []

    if (this.previous) {
      const previous = this.previous
      this.previous = null
      closing.push(previous.close())
    }

    if (this.current) {
      const current = this.current
      this.current = null
      closing.push(current.close())
    }

    return Promise.all(closing)
  }
}

function defaultWatchMap (snapshot) {
  return snapshot
}

async function leafSize (node, goLeft) {
  while (node.children.length) node = await node.getChildNode(goLeft ? 0 : node.children.length - 1)
  return node.keys.length
}

async function setKeyToNearestLeaf (node, index, stack) {
  let [left, right] = await Promise.all([node.getChildNode(index), node.getChildNode(index + 1)])
  const [ls, rs] = await Promise.all([leafSize(left, false), leafSize(right, true)])

  if (ls < rs) { // if fewer leaves on the left
    stack.push(right)
    while (right.children.length) stack.push(right = right.children[0].value)
    node.keys[index] = right.keys.shift()
  } else { // if fewer leaves on the right
    stack.push(left)
    while (left.children.length) stack.push(left = left.children[left.children.length - 1].value)
    node.keys[index] = left.keys.pop()
  }
}

async function rebalance (stack) {
  const root = stack[0]

  while (stack.length > 1) {
    const node = stack.pop()
    const parent = stack[stack.length - 1]

    if (node.keys.length >= MIN_KEYS) return root

    let { left, index, right } = await node.siblings(parent)

    // maybe borrow from left sibling?
    if (left && left.keys.length > MIN_KEYS) {
      left.changed = true
      node.keys.unshift(parent.keys[index - 1])
      if (left.children.length) node.children.unshift(left.children.pop())
      parent.keys[index - 1] = left.keys.pop()
      return root
    }

    // maybe borrow from right sibling?
    if (right && right.keys.length > MIN_KEYS) {
      right.changed = true
      node.keys.push(parent.keys[index])
      if (right.children.length) node.children.push(right.children.shift())
      parent.keys[index] = right.keys.shift()
      return root
    }

    // merge node with another sibling
    if (left) {
      index--
      right = node
    } else {
      left = node
    }

    left.merge(right, parent.keys[index])
    parent.removeKey(index)
  }

  // check if the tree shrunk
  if (!root.keys.length && root.children.length) return root.getChildNode(0)
  return root
}

function iteratorToStream (ite) {
  let done
  let closing

  const rs = new Readable({
    open (cb) {
      done = cb
      ite.open().then(fin, fin)
    },
    read (cb) {
      done = cb
      ite.next().then(push, fin)
    },
    predestroy () {
      closing = ite.close()
      closing.catch(noop)
    },
    destroy (cb) {
      done = cb
      if (!closing) closing = ite.close()
      closing.then(fin, fin)
    }
  })

  return rs

  function fin (err) {
    process.nextTick(done, err)
  }

  function push (val) {
    process.nextTick(pushNT, val)
  }

  function pushNT (val) {
    rs.push(val)
    done(null)
  }
}

async function iteratorPeek (ite) {
  try {
    await ite.open()
    return await ite.next()
  } finally {
    await ite.close()
  }
}

function encRange (e, opts) {
  if (!e) return opts
  if (opts.gt !== undefined) opts.gt = enc(e, opts.gt)
  if (opts.gte !== undefined) opts.gte = enc(e, opts.gte)
  if (opts.lt !== undefined) opts.lt = enc(e, opts.lt)
  if (opts.lte !== undefined) opts.lte = enc(e, opts.lte)
  if (opts.sub && !opts.gt && !opts.gte) opts.gt = enc(e, SEP)
  if (opts.sub && !opts.lt && !opts.lte) opts.lt = bump(enc(e, EMPTY))
  return opts
}

function bump (key) {
  // key should have been copied by enc above before hitting this
  key[key.length - 1]++
  return key
}

function enc (e, v) {
  if (v === undefined || v === null) return null
  if (e !== null) return e.encode(v)
  if (typeof v === 'string') return b4a.from(v)
  return v
}

function prefixEncoding (prefix, keyEncoding) {
  return {
    encode (key) {
      return b4a.concat([prefix, b4a.isBuffer(key) ? key : enc(keyEncoding, key)])
    },
    decode (key) {
      const sliced = key.slice(prefix.length, key.length)
      return keyEncoding ? keyEncoding.decode(sliced) : sliced
    }
  }
}

function defaultDiffer (currentSnap, previousSnap, opts) {
  return currentSnap.createDiffStream(previousSnap.version, opts)
}

function noop () {}

module.exports = Hyperbee
