const codecs = require('codecs')
const { Readable } = require('streamx')
const mutexify = require('mutexify/promise')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')
const ReadyResource = require('ready-resource')
const debounce = require('debounceify')
const Rache = require('rache')

const { all: unslabAll } = require('unslab')

const RangeIterator = require('./iterators/range')
const HistoryIterator = require('./iterators/history')
const DiffIterator = require('./iterators/diff')
const Extension = require('./lib/extension')
const { YoloIndex, Node, Header } = require('./lib/messages')
const { BLOCK_NOT_AVAILABLE, DECODING_ERROR } = require('hypercore-errors')

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

class Cache {
  constructor (rache) {
    this.keys = rache
    this.length = 0
  }

  get (seq) {
    return this.keys.get(seq) || null
  }

  set (seq, key) {
    this.keys.set(seq, key)
    if (seq >= this.length) this.length = seq + 1
  }

  gc (length) {
    // if we need to "work" more than 128 ticks, just bust the cache...
    if (this.length - length > 128) {
      this.keys.clear()
    } else {
      for (let i = length; i < this.length; i++) {
        this.keys.delete(i)
      }
    }

    this.length = length
  }
}

class Pointers {
  constructor (decoded) {
    this.levels = decoded.levels.map(l => {
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

function inflate (entry) {
  if (entry.inflated === null) {
    entry.inflated = YoloIndex.decode(entry.index)
    entry.index = null
  }
  return new Pointers(entry.inflated)
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

function preloadBlock (core, index) {
  if (core.replicator._blocks.get(index)) return
  core.get(index).catch(safetyCatch)
}

class TreeNode {
  constructor (block, keys, children, offset) {
    this.block = block
    this.offset = offset
    this.keys = keys
    this.children = children
    this.changed = false

    this.preload()
  }

  preload () {
    if (this.block === null) return

    const core = getBackingCore(this.block.tree.core)
    const indexedLength = getIndexedLength(this.block.tree.core)
    const bitfield = core.core.bitfield

    for (let i = 0; i < this.keys.length; i++) {
      const k = this.keys[i]
      if (k.value) continue
      if (k.seq >= indexedLength || bitfield.get(k.seq)) continue
      preloadBlock(core, k.seq)
    }
    for (let i = 0; i < this.children.length; i++) {
      const c = this.children[i]
      if (c.value) continue
      if (c.seq >= indexedLength || bitfield.get(c.seq)) continue
      preloadBlock(core, c.seq)
    }
  }

  async insertKey (key, value, child, node, encoding, cas) {
    let s = 0
    let e = this.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = b4a.compare(key.value, await this.getKey(mid))

      if (c === 0) {
        if (cas) {
          const prev = await this.getKeyNode(mid)
          if (!(await cas(prev.final(encoding), node))) return true
        }
        if (!this.block.tree.tree.alwaysDuplicate) {
          const prev = await this.getKeyNode(mid)
          if (sameValue(prev.value, value)) return true
        }
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

  updateChildren (seq, block) {
    for (const child of this.children) {
      if (!child.value || child.seq !== seq) continue
      child.value.block = block
      child.value.updateChildren(seq, block)
    }
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
    this.entry = entry
    this.key = entry.key
    this.value = entry.value
  }

  isTarget (key) {
    return b4a.equals(this.key, key)
  }

  isDeletion () {
    if (this.value !== null) return false

    if (this.index === null) {
      this.index = inflate(this.entry)
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
      this.index = inflate(this.entry)
    }
    const entry = this.index.get(offset)
    return new TreeNode(this, entry.keys, entry.children, offset)
  }
}

class BatchEntry extends BlockEntry {
  constructor (seq, tree, key, value, index) {
    super(seq, tree, { key, value, index: null, inflated: null })
    this.pendingIndex = index
  }

  isTarget (key) {
    return false
  }

  getTreeNode (offset) {
    return this.pendingIndex[offset].value
  }
}

class Hyperbee extends ReadyResource {
  constructor (core, opts = {}) {
    super()
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = core
    this.core = core

    this.keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : null
    this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
    this.extension = opts.extension !== false ? opts.extension || Extension.register(this) : null
    this.metadata = opts.metadata || null
    this.lock = opts.lock || mutexify()
    this.sep = opts.sep || SEP
    this.readonly = !!opts.readonly
    this.prefix = opts.prefix || null

    // In a future version, this should be false by default
    this.alwaysDuplicate = opts.alwaysDuplicate !== false

    this._unprefixedKeyEncoding = this.keyEncoding
    this._sub = !!this.prefix
    this._checkout = opts.checkout || 0
    this._view = !!opts._view

    this._onappendBound = this._view ? null : this._onappend.bind(this)
    this._ontruncateBound = this._view ? null : this._ontruncate.bind(this)
    this._watchers = this._onappendBound ? [] : null
    this._entryWatchers = this._onappendBound ? [] : null
    this._sessions = opts.sessions !== false

    this._keyCache = null
    this._nodeCache = null

    this._batches = []

    if (this._watchers) {
      this.core.on('append', this._onappendBound)
      this.core.on('truncate', this._ontruncateBound)
    }

    if (this.prefix && opts._sub) {
      this.keyEncoding = prefixEncoding(this.prefix, this.keyEncoding)
    }

    this.ready().catch(safetyCatch)
  }

  async _open () {
    if (this.core.opened === false) await this.core.ready()

    // snapshot
    if (this._checkout === -1) this._checkout = Math.max(1, this.core.length)

    const baseCache = Rache.from(this.core.globalCache)
    this._keyCache = new Cache(baseCache)
    this._nodeCache = new Cache(Rache.from(baseCache))
  }

  get version () {
    return Math.max(1, this._checkout || this.core.length)
  }

  get id () {
    return this.core.id
  }

  get key () {
    return this.core.key
  }

  get discoveryKey () {
    return this.core.discoveryKey
  }

  get writable () {
    return this.core.writable
  }

  get readable () {
    return this.core.readable
  }

  replicate (isInitiator, opts) {
    return this.core.replicate(isInitiator, opts)
  }

  update (opts) {
    return this.core.update(opts)
  }

  peek (range, opts) {
    return iteratorPeek(this.createRangeIterator(range, { ...opts, limit: 1 }))
  }

  createRangeIterator (range, opts = {}) {
    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

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

  createReadStream (range, opts) {
    const signal = (opts && opts.signal) || null
    return iteratorToStream(this.createRangeIterator(range, opts), signal)
  }

  createHistoryStream (opts) {
    const session = (opts && opts.live) ? this.core.session() : this._makeSnapshot()
    const signal = (opts && opts.signal) || null
    return iteratorToStream(new HistoryIterator(new Batch(this, session, null, false, opts), opts), signal)
  }

  createDiffStream (right, range, opts) {
    if (typeof right === 'number') right = this.checkout(Math.max(1, right), { reuseSession: true })

    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const snapshot = right.version > this.version ? right._makeSnapshot() : this._makeSnapshot()
    const signal = (opts && opts.signal) || null

    const keyEncoding = opts && opts.keyEncoding ? codecs(opts.keyEncoding) : this.keyEncoding
    if (keyEncoding) opts = encRange(keyEncoding, { ...opts, sub: this._sub })

    return iteratorToStream(new DiffIterator(new Batch(this, snapshot, null, false, opts), new Batch(right, snapshot, null, false, opts), opts), signal)
  }

  get (key, opts) {
    const b = new Batch(this, this._makeSnapshot(), null, true, opts)
    return b.get(key)
  }

  getBySeq (seq, opts) {
    const b = new Batch(this, this._makeSnapshot(), null, true, opts)
    return b.getBySeq(seq)
  }

  put (key, value, opts) {
    const b = new Batch(this, this.core, null, true, opts)
    return b.put(key, value, opts)
  }

  batch (opts) {
    return new Batch(this, this.core, mutexify(), true, opts)
  }

  del (key, opts) {
    const b = new Batch(this, this.core, null, true, opts)
    return b.del(key, opts)
  }

  watch (range, opts) {
    if (!this._watchers) throw new Error('Can only watch the main bee instance')
    return new Watcher(this, range, opts)
  }

  async getAndWatch (key, opts) {
    if (!this._watchers) throw new Error('Can only watch the main bee instance')

    const watcher = new EntryWatcher(this, key, opts)
    await watcher._debouncedUpdate()

    if (this.closing) {
      await watcher.close()
      throw new Error('Bee closed')
    }

    return watcher
  }

  _onappend () {
    for (const watcher of this._watchers) {
      watcher._onappend()
    }

    for (const watcher of this._entryWatchers) {
      watcher._onappend()
    }
  }

  _ontruncate (length) {
    for (const watcher of this._watchers) {
      watcher._ontruncate()
    }

    for (const watcher of this._entryWatchers) {
      watcher._ontruncate()
    }

    this._nodeCache.gc(length)
    this._keyCache.gc(length)
  }

  _makeSnapshot () {
    if (this._sessions === false) return this.core
    // TODO: better if we could encapsulate this in hypercore in the future
    return (this._checkout <= this.core.length || this._checkout <= 1) ? this.core.snapshot() : this.core.session({ snapshot: false })
  }

  checkout (version, opts = {}) {
    if (version === 0) version = 1

    // same as above, just checkout isn't set yet...
    const snap = (opts.reuseSession || this._sessions === false)
      ? this.core
      : (version <= this.core.length || version <= 1) ? this.core.snapshot() : this.core.session({ snapshot: false })

    return new Hyperbee(snap, {
      _view: true,
      _sub: false,
      prefix: this.prefix,
      sep: this.sep,
      lock: this.lock,
      checkout: version,
      keyEncoding: opts.keyEncoding || this.keyEncoding,
      valueEncoding: opts.valueEncoding || this.valueEncoding,
      extension: this.extension !== null ? this.extension : false
    })
  }

  snapshot (opts) {
    return this.checkout(this.core.opened === false ? -1 : Math.max(1, this.version), opts)
  }

  sub (prefix, opts = {}) {
    let sep = opts.sep || this.sep
    if (!b4a.isBuffer(sep)) sep = b4a.from(sep)

    prefix = b4a.concat([this.prefix || EMPTY, b4a.from(prefix), sep])

    const valueEncoding = codecs(opts.valueEncoding || this.valueEncoding)
    const keyEncoding = codecs(opts.keyEncoding || this._unprefixedKeyEncoding)

    return new Hyperbee(this.core, {
      _view: true,
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
    const blk = await this.core.get(0, opts)
    try {
      return blk && Header.decode(blk)
    } catch {
      throw DECODING_ERROR()
    }
  }

  async _close () {
    if (this._watchers) {
      this.core.off('append', this._onappendBound)
      this.core.off('truncate', this._ontruncateBound)

      while (this._watchers.length) {
        await this._watchers[this._watchers.length - 1].close()
      }
    }

    if (this._entryWatchers) {
      while (this._entryWatchers.length) {
        await this._entryWatchers[this._entryWatchers.length - 1].close()
      }
    }

    while (this._batches.length) {
      await this._batches[this._batches.length - 1].close()
    }

    return this.core.close()
  }

  static async isHyperbee (core, opts) {
    await core.ready()

    const blk0 = await core.get(0, opts)
    if (blk0 === null) throw BLOCK_NOT_AVAILABLE()

    try {
      return Header.decode(blk0).protocol === 'hyperbee'
    } catch (err) { // undecodable
      return false
    }
  }
}

class Batch {
  constructor (tree, core, batchLock, cache, options = {}) {
    this.tree = tree
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = core
    this.core = core
    this.index = tree._batches.push(this) - 1
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
    this.isSnapshot = this.core !== this.tree.core
    this.shouldUpdate = this.options.update !== false
    this.updating = null
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
    return Math.max(1, this.tree._checkout ? this.tree._checkout : this.core.length + this.length)
  }

  async getRoot (ensureHeader) {
    await this.ready()
    if (ensureHeader) {
      if (this.core.length === 0 && this.core.writable && !this.tree.readonly) {
        await this.core.append(Header.encode({
          protocol: 'hyperbee',
          metadata: this.tree.metadata
        }))
      }
    }
    if (this.tree._checkout === 0 && this.shouldUpdate) {
      if (this.updating === null) this.updating = this.core.update()
      await this.updating
    }
    if (this.version < 2) return null
    return (await this.getBlock(this.version - 1)).getTreeNode(0)
  }

  async getKey (seq) {
    const k = this.core.fork === this.tree.core.fork ? this.tree._keyCache.get(seq) : null
    if (k !== null) return k
    const key = (await this.getBlock(seq)).key
    if (this.core.fork === this.tree.core.fork) this.tree._keyCache.set(seq, key)
    return key
  }

  async _getNode (seq) {
    const cached = this.tree._nodeCache !== null && this.core.fork === this.tree.core.fork ? this.tree._nodeCache.get(seq) : null
    if (cached !== null) return cached
    const entry = await this.core.get(seq, { ...this.options, valueEncoding: Node })
    if (entry === null) throw BLOCK_NOT_AVAILABLE()
    const wrap = copyEntry(entry)
    if (this.core.fork === this.tree.core.fork && this.tree._nodeCache !== null) this.tree._nodeCache.set(seq, wrap)
    return wrap
  }

  async getBlock (seq) {
    if (this.rootSeq === 0) this.rootSeq = seq
    let b = this.blocks && this.blocks.get(seq)
    if (b) return b
    this.onseq(seq)
    const entry = await this._getNode(seq)
    b = new BlockEntry(seq, this, entry)
    if (this.blocks && (this.blocks.size - this.length) < 128) this.blocks.set(seq, b)
    return b
  }

  _onwait (key) {
    this.options.onwait = null
    this.tree.extension.get(this.rootSeq + 1, key)
  }

  _getEncoding (opts) {
    if (!opts) return this.encoding
    return {
      key: opts.keyEncoding ? codecs(opts.keyEncoding) : this.encoding.key,
      value: opts.valueEncoding ? codecs(opts.valueEncoding) : this.encoding.value
    }
  }

  peek (range, opts) {
    return iteratorPeek(this.createRangeIterator(range, { ...opts, limit: 1 }))
  }

  createRangeIterator (range, opts = {}) {
    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const encoding = this._getEncoding(opts)
    return new RangeIterator(this, encoding, encRange(encoding.key, { ...opts, sub: this.tree._sub }))
  }

  createReadStream (range, opts) {
    const signal = (opts && opts.signal) || null
    return iteratorToStream(this.createRangeIterator(range, opts), signal)
  }

  async getBySeq (seq, opts) {
    const encoding = this._getEncoding(opts)

    try {
      const block = (await this.getBlock(seq)).final(encoding)
      return { key: block.key, value: block.value }
    } finally {
      await this._closeSnapshot()
    }
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
      if (node.block.isTarget(key)) {
        return node.block.isDeletion() ? null : node.block.final(encoding)
      }

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

    const seq = newNode.seq = this.core.length + this.length
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
          if (cas) {
            const prev = await node.getKeyNode(mid)
            if (!(await cas(prev.final(encoding), newNode))) return this._unlockMaybe()
          }
          if (!this.tree.alwaysDuplicate) {
            const prev = await node.getKeyNode(mid)
            if (sameValue(prev.value, value)) return this._unlockMaybe()
          }
          node.setKey(mid, target)
          return this._append(root, seq, key, value)
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }

    let needsSplit = !(await node.insertKey(target, value, null, newNode, encoding, cas))
    if (!node.changed) return this._unlockMaybe()

    while (needsSplit) {
      const parent = stack.pop()
      const { median, right } = await node.split()

      if (parent) {
        needsSplit = !(await parent.insertKey(median, value, right, null, encoding, null))
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

    const seq = delNode.seq = this.core.length + this.length

    while (true) {
      stack.push(node)

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(key, await node.getKey(mid))

        if (c === 0) {
          if (cas) {
            const prev = await node.getKeyNode(mid)
            if (!(await cas(prev.final(encoding), delNode))) return this._unlockMaybe()
          }
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
    if (this.isSnapshot) {
      await this.core.close()
      this._finalize()
    }
  }

  async close () {
    if (this.isSnapshot) return this._closeSnapshot()

    this.root = null
    if (this.blocks) this.blocks.clear()
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
      const seq = this.core.length + i
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
    this._finalize()
  }

  _finalize () {
    // technically finalize can be called more than once, so here we just check if we already have been removed
    if (this.index >= this.tree._batches.length || this.tree._batches[this.index] !== this) return
    const top = this.tree._batches.pop()
    if (top === this) return
    top.index = this.index
    this.tree._batches[top.index] = top
  }

  _append (root, seq, key, value) {
    const index = []
    root.indexChanges(index, seq)
    index[0] = new Child(seq, 0, root)

    if (!this.autoFlush) {
      const block = new BatchEntry(seq, this, key, value, index)
      root.block = block
      this.root = root
      this.length++
      this.blocks.set(seq, block)

      root.updateChildren(seq, block)
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
      await this.core.append(raw)
    } finally {
      this._unlock()
    }
  }
}

class EntryWatcher extends ReadyResource {
  constructor (bee, key, opts = {}) {
    super()

    this.keyEncoding = opts.keyEncoding || bee.keyEncoding
    this.valueEncoding = opts.valueEncoding || bee.valueEncoding

    this.index = bee._entryWatchers.push(this) - 1
    this.bee = bee

    this.key = key
    this.node = null

    this._forceUpdate = false
    this._debouncedUpdate = debounce(this._processUpdate.bind(this))
    this._updateOnce = !!opts.updateOnce
  }

  _close () {
    const top = this.bee._entryWatchers.pop()
    if (top !== this) {
      top.index = this.index
      this.bee._entryWatchers[top.index] = top
    }
  }

  _onappend () {
    this._debouncedUpdate()
  }

  _ontruncate () {
    this._forceUpdate = true
    this._debouncedUpdate()
  }

  async _processUpdate () {
    const force = this._forceUpdate
    this._forceUpdate = false

    if (this._updateOnce) {
      this._updateOnce = false
      await this.bee.update({ wait: true })
    }

    let newNode
    try {
      newNode = await this.bee.get(this.key, {
        keyEncoding: this.keyEncoding,
        valueEncoding: this.valueEncoding
      })
    } catch (e) {
      if (e.code === 'SNAPSHOT_NOT_AVAILABLE') {
        // There was a truncate event before the get resolved
        // So this handler will run again anyway
        return
      } else if (this.bee.closing) {
        this.close().catch(safetyCatch)
        return
      }
      this.emit('error', e)
      return
    }

    if (force || newNode?.seq !== this.node?.seq) {
      this.node = newNode
      this.emit('update')
    }
  }
}

class Watcher extends ReadyResource {
  constructor (bee, range, opts = {}) {
    super()

    this.keyEncoding = opts.keyEncoding || bee.keyEncoding
    this.valueEncoding = opts.valueEncoding || bee.valueEncoding
    this.index = bee._watchers.push(this) - 1
    this.bee = bee
    this.core = bee.core

    this.latestDiff = 0
    this.range = range
    this.map = opts.map || defaultWatchMap

    this.current = null
    this.previous = null
    this.currentMapped = null
    this.previousMapped = null
    this.stream = null

    this._lock = mutexify()
    this._flowing = false
    this._resolveOnChange = null
    this._differ = opts.differ || defaultDiffer
    this._eager = !!opts.eager
    this._updateOnce = !!opts.updateOnce
    this._onchange = opts.onchange || null
    this._flush = opts.flush !== false && this.core.isAutobase

    this.on('newListener', autoFlowOnUpdate)

    this.ready().catch(safetyCatch)
  }

  async _consume () {
    if (this._flowing) return
    try {
      for await (const _ of this) {} // eslint-disable-line
    } catch {}
  }

  async _open () {
    await this.bee.ready()

    const opts = {
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding
    }

    // Point from which to start watching
    this.current = this._eager ? this.bee.checkout(1, opts) : this.bee.snapshot(opts)

    if (this._onchange) {
      if (this._eager) await this._onchange()
      this._consume()
    }
  }

  [Symbol.asyncIterator] () {
    this._flowing = true
    return this
  }

  _ontruncate () {
    if (this.core.isAutobase) this._onappend()
  }

  _onappend () {
    // TODO: this is a light hack / fix for non-sparse session reporting .length's inside batches
    // the better solution is propably just to change non-sparse sessions to not report a fake length
    if (!this.closing && !this.core.isAutobase && (!this.core.core || this.core.core.tree.length !== this.core.length)) return

    const resolve = this._resolveOnChange
    this._resolveOnChange = null
    if (resolve) resolve()
  }

  async _waitForChanges () {
    if (this.current.version < this.bee.version || this.closing) return

    await new Promise(resolve => {
      this._resolveOnChange = resolve
    })
  }

  async next () {
    try {
      return await this._next()
    } catch (err) {
      if (this.closing) return { value: undefined, done: true }
      await this.close()
      throw err
    }
  }

  async _next () {
    const release = await this._lock()

    try {
      if (this.closing) return { value: undefined, done: true }

      if (!this.opened) await this.ready()

      while (true) {
        await this._waitForChanges()

        if (this.closing) return { value: undefined, done: true }

        if (this._updateOnce) {
          this._updateOnce = false
          await this.bee.update({ wait: true })
        }

        if (this._flush) await this.core.base.flush()
        if (this.closing) return { value: undefined, done: true }

        await this._closePrevious()
        this.previous = this.current.snapshot()

        await this._closeCurrent()
        this.current = this.bee.snapshot({
          keyEncoding: this.keyEncoding,
          valueEncoding: this.valueEncoding
        })

        if (this.current.core.fork !== this.previous.core.fork) {
          return await this._yield()
        }

        this.stream = this._differ(this.current, this.previous, this.range)

        try {
          for await (const data of this.stream) { // eslint-disable-line
            return await this._yield()
          }
        } finally {
          this.stream = null
        }
      }
    } finally {
      release()
    }
  }

  async _yield () {
    this.currentMapped = this.map(this.current)
    this.previousMapped = this.map(this.previous)

    if (this._onchange) {
      try {
        await this._onchange()
      } catch (err) {
        safetyCatch(err)
      }
    }

    this.emit('update')
    return { done: false, value: [this.currentMapped, this.previousMapped] }
  }

  async return () {
    await this.close()
    return { done: true }
  }

  async _close () {
    const top = this.bee._watchers.pop()
    if (top !== this) {
      top.index = this.index
      this.bee._watchers[top.index] = top
    }

    if (this.stream && !this.stream.destroying) {
      this.stream.destroy()
    }

    this._onappend() // Continue execution being closed

    await this._closeCurrent().catch(safetyCatch)
    await this._closePrevious().catch(safetyCatch)

    const release = await this._lock()
    release()
  }

  destroy () {
    return this.close()
  }

  async _closeCurrent () {
    if (this.currentMapped) await this.currentMapped.close()
    if (this.current) await this.current.close()
    this.current = this.currentMapped = null
  }

  async _closePrevious () {
    if (this.previousMapped) await this.previousMapped.close()
    if (this.previous) await this.previous.close()
    this.previous = this.previousMapped = null
  }
}

function autoFlowOnUpdate (name) {
  if (name === 'update') this._consume()
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

function iteratorToStream (ite, signal) {
  let done
  let closing

  const rs = new Readable({
    signal,
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
    done(err)
  }

  function push (val) {
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

  if (e.encodeRange) {
    const r = e.encodeRange({ gt: opts.gt, gte: opts.gte, lt: opts.lt, lte: opts.lte })
    opts.gt = r.gt
    opts.gte = r.gte
    opts.lt = r.lt
    opts.lte = r.lte
    return opts
  }

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

function copyEntry (entry) {
  let key = entry.key
  let value = entry.value
  let index = entry.index

  // key, value and index all refer to the same buffer (one hypercore block)
  // If together they are larger than half the buffer's byteLength,
  // this means that they got their own private slab (see Buffer.allocUnsafe docs)
  // so no need to unslab
  const size = key.byteLength + (value === null ? 0 : value.byteLength) + (index === null ? 0 : index.byteLength)
  if (2 * size < key.buffer.byteLength) {
    const [newKey, newValue, newIndex] = unslabAll([entry.key, entry.value, entry.index])
    key = newKey
    value = newValue
    index = newIndex
  }

  return {
    key,
    value,
    index,
    inflated: null
  }
}

function defaultDiffer (currentSnap, previousSnap, opts) {
  return currentSnap.createDiffStream(previousSnap, opts)
}

function getBackingCore (core) {
  if (core._source) return core._source.originalCore
  if (core.flush) return core.session
  return core
}

function getIndexedLength (core) {
  if (core._source) return core._source.core.indexedLength
  if (core.flush) return core.indexedLength
  return core.length
}

function sameValue (a, b) {
  return a === b || (a !== null && b !== null && b4a.equals(a, b))
}

function noop () {}

module.exports = Hyperbee
