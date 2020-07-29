const MAX_CHILDREN = 4
let debug = false

class KeyCache {
  constructor (feed) {
    this.feed = feed
    this.cache = new Map()
  }

  preload (seqs) {
    // TODO when async
  }

  clear () {
    this.cache.clear()
  }

  set (seq, key) {
    this.cache.set(seq, key)
  }

  async get (seq) {
    return this.cache.get(seq) || (await getFeed(this.feed, seq)).key
  }
}

const cache = new Map()


function getFeed (feed, i) {
  return new Promise((resolve, reject) => {
    feed.get(i, (err, val) => {
      if (err) return reject(err)
      resolve(val)
    })
  })
}

function appendFeed (feed, val) {
  return new Promise((resolve, reject) => {
    feed.append(val, (err) => {
      if (err) return reject(err)
      resolve()
    })
  })
}

class BTree {
  constructor (feed) {
    this.feed = feed
    this.keys = new KeyCache(feed)
  }

  async getNode ([seq, index]) {
    const entry = decompress(await getFeed(this.feed, seq))
    return new Node(seq, index, entry, this)
  }

  async getRoot () {
    await this.ready()
    if (this.feed.length < 2) return null
    return await this.getNode([this.feed.length - 1, 0], null)
  }

  async ready () {
    await new Promise((resolve, reject) => {
      this.feed.ready(err => {
        if (err) return reject(err)
        resolve()
      })
    })

    if (!this.feed.length) {
      await appendFeed(this.feed, null)
    }
  }

  get (key) {

  }

  async put (key) {
    await this.ready()

    this.keys.clear()

    const index = []
    const stack = []
    const seq = this.feed.length

    let root
    let node = root = await this.getRoot()

    if (!node) {
      await appendFeed(this.feed, {
        seq,
        key,
        index: [
          { keys: [1], children: [] }
        ]
      })
      return
    }

    while (node.childCount()) {
      stack.push(node)
      node.changed = true // changed, but compressible

      const keys = await node.keys()
      let next

      for (let i = 0; i < keys.length; i++) { // TODO: bisect
        const k = keys[i]

        if (k === key) {
          console.log('exact match')
          process.exit(1)
        }

        if (key < k) {
          next = await node.child(i)
          break
        }
      }

      node = next || await node.child(keys.length)
    }

    let needsSplit = !(await node.addKey(key, seq))

    if (needsSplit) this.keys.set(seq, key)

    while (needsSplit) {
      // if (debug) console.log('splits', node._keys.map(k => this.feed[k] && this.feed[k].key || key))
      const parent = stack.pop()
      const { median, right } = await node.split()

      if (parent) {
        needsSplit = !(await parent.addChild(median, right))
        node = parent
      } else {
        root = Node.create(this)
        root.changed = true
        root._keys = [median]
        root._children = [node, right]
        needsSplit = false
      }
    }

    await indexify(root, index, seq)

    await appendFeed(this.feed, compress({
      seq,
      key,
      index
    }))
  }

  async stringify () {
    const root = await this.getRoot()
    const m = await load(root)

    return require('tree-to-string')(m)

    async function load (n) {
      const r = {}
      r.values = await n.keys()
      r.children = []
      for (const c of await n.children()) {
        r.children.push(await load(c))
      }
      return r
    }
  }
}

class Node {
  constructor (seq, index, entry, tree) {
    if (entry === undefined) console.trace(entry, seq)
    this.seq = seq
    this.index = index
    this.changed = false
    this._entry = entry
    this._children = null
    this._keys = entry ? entry.index[index].keys : []
    this._tree = tree
  }

  async loadAll () {
    if (this._children) return

    const idx = this._entry.index[this.index].children
    this._children = []

    for (const ptr of idx) {
      this._children.push(await this._tree.getNode(ptr))
    }

    this._tree.keys.preload(this.keys)
  }

  async keys () {
    const keys = new Array(this._keys.length)

    this._tree.keys.preload(this._keys)
    for (let i = 0; i < keys.length; i++) {
      keys[i] = await this._tree.keys.get(this._keys[i])
    }

    return keys
  }

  async addKey (key, keyPtr) {
    const keys = await this.keys()
    keys.push(key)
    keys.sort(cmp)

    const i = keys.indexOf(key)
    if (this._entry && this._keys === this._entry.index[this.index].keys) {
      this._keys = this._keys.slice(0)
    }
    this._keys.splice(i, 0, keyPtr)
    this.changed = true
    return keys.length < MAX_CHILDREN
  }

  async addChild (keyPtr, child) {
    let i = 0

    const key = await this._tree.keys.get(keyPtr)
    const keys = await this.keys()

    for (; i < keys.length; i++) { // TODO: bisect
      const v = keys[i]
      if (key < v) break
    }

    await this.loadAll() // TODO: allow sparse loading

    this._keys.splice(i, 0, keyPtr)
    this._children.splice(i + 1, 0, child)
    this.changed = true

    return this._keys.length < MAX_CHILDREN
  }

  childCount () {
    return this._children ? this._children.length : this._entry.index[this.index].children.length
  }

  async children () {
    await this.loadAll()
    return this._children
  }

  async child (i) {
    await this.loadAll()
    return this._children[i]
  }

  async split () {
    await this.loadAll()

    const len = (this._keys.length / 2) | 0
    const right = Node.create(this._tree)

    while (right._keys.length < len) right._keys.push(this._keys.pop())
    right._keys.reverse()

    const median = this._keys.pop()

    if (this._children.length) {
      while (right._children.length < len + 1) right._children.push(this._children.pop())
      right._children.reverse()
    }

    this.changed = true

    return {
      left: this,
      median,
      right
    }
  }

  static create (tree) {
    const node = new Node(0, 0, null, tree)
    node.changed = true
    node._children = []
    return node
  }
}

async function indexify (node, index, seq) {
  const i = index.push(null) - 1
  const keys = node._keys.slice(0)

  const children = []

  for (const child of (await node.children())) {
    if (!child.changed) {
      children.push([child.seq, child.index])
    } else {
      const i = await indexify(child, index, seq)
      children.push([seq, i])
    }
  }

  index[i] = { keys, children }
  return i
}

function cmp (a, b) {
  return a < b ? -1 : a > b ? 1 : 0
}

function decompress (node) {
  const index = []
  for (const idx of node.index) {
    const children = []
    for (const ptr of idx.children) children.push([node.seq - ptr[0], ptr[1]])
    index.push({ keys: idx.keys, children })
  }
  return { ...node, index }
}

function compress (node) {
  const index = []
  for (const idx of node.index) {
    const children = []
    for (const ptr of idx.children) children.push([node.seq - ptr[0], ptr[1]])
    index.push({ keys: idx.keys, children })
  }
  return { ...node, index }
}

const Cache = require('hypercore-cache')
const hypercore = require('hypercore')
const t = new BTree(hypercore('data', { cache: { data: 128 * 1024 * 1024 }, valueEncoding: 'json', crypto: { verify (a, b, c, cb) { cb(null, true) }, sign (a, b, cb) { cb(null, Buffer.alloc(32)) }} }))
let n = 0
let mag = 2
let max = 0

let wait = 0

process.once('SIGINT', function () {
  wait = 3600 * 1000
})

async function push (l = 1) {
  await t.ready()
  console.log(t.feed.length, indexSize((await getFeed(t.feed, t.feed.length - 1)).index))

  for (let i = 0; i < l; i++) {
    // t.put('#' + (n++).toString().padStart(5, '0'))
    // console.log(i)
    const then = Date.now()
    await t.put(Math.random().toString(16).slice(2))
    const d = Date.now() - then
    if (d > max) console.log('max put time:', max = d)
    if (++n >= mag) {
      mag *= 2
      console.log(n, t.feed.length, indexSize((await getFeed(t.feed, t.feed.length - 1)).index))
    }

    if (wait) await new Promise(r => setTimeout(r, wait))
  }
}

// push(1e6)
push(1e6).then(async () => {
  // console.log(await t.stringify())
})

// push(10)
// push(10)

require('util').inspect.defaultOptions.depth = Infinity

function indexSize (index) {
  const varint = require('varint')
  const n = index.map(({ keys, children }) => {
    let r = 1
    for (const k of keys) r += varint.encodingLength(k)
    for (const [d, i] of children) r += varint.encodingLength(d) + 1
    return r
  })

  return 1 + // index.length
    Math.ceil(n.length / 8) + // isLeafs
    n.reduce((a, b) => a + b, 0)
}

// console.log(t.feed[t.feed.length - 1])
// console.log(indexSize(t.feed[t.feed.length - 1].index))
// t.put('a')
// t.put('b')
// t.put('c')
// t.put('d')
// t.put('e')
// t.put('f')
// // console.log(t + '')
// // debug = true
// t.put('g')
// t.put('h')
// t.put('i')
// t.put('j')


// console.log(t + '')
// console.log(t.feed)
