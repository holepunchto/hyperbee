/*

{
  seq: 0,
  keys: [1, 4],
  children: [[1, 4], [2, 3], [1, 3]]
}
{

}

{
  tree: [
    [
      [ ... ],
      [ ... ],
      [ ... ]
    ],
    [
    ]
  ]
}

*/

let dirty = []

class Node {
  constructor () {
    this.values = []
    this.children = []
  }

  markDirty () {
    if (dirty.includes(this)) return this
    dirty.push(this)
    return this
  }

  toString () {
    return require('tree-to-string')(this)
  }
}

// let debug = false
// let root = new Node()

// const feed = []

// insert(1)
// console.log(lookup(1))
// insert(2)
// insert(3)
// insert(4)
// console.log(require('tree-to-string')(root))
// insert(5)
// console.log(require('tree-to-string')(root))
// debug = true
// insert(6)
// console.log(require('tree-to-string')(root))
// insert(7)
// console.log(require('tree-to-string')(root))
// insert(8)
// insert(9)
// insert(10)
// insert(11)
// insert(2.4)
// insert(2.5)
// insert(2.6)
// insert(2.7)
// console.log(root + '')

// function splitNode (node) {
//   const mid = node.values[1]
//   const right = new Node()
//   right.values.push(node.values.pop())
//   node.values.pop()

//   if (node.children.length) {
//     right.children.push(node.children[2])
//     right.children.push(node.children[3])
//     node.children.pop()
//     node.children.pop()
//   }

//   return {
//     left: node,
//     median: mid,
//     right
//   }
// }

// function lookup (val) {
//   const stack = [root]

//   let node = root

//   while (node.children.length) {
//     if (node.values.indexOf(val) > -1) return node

//     if (val < node.values[0]) {
//       node = node.children[0]
//     } else if (node.children.length === 2 || val < node.values[1]) {
//       node = node.children[1]
//     } else {
//       node = node.children[2]
//     }

//     stack.push(node)
//   }

//   return stack
// }

class FeedNode {
  constructor (feed, node, seq, index) {
    this.node = node
    this.feed = feed
    this.seq = seq
    this.index = index
    this.changed = false

    this._keys = null
    this._keyIds = null
    this._children = null
  }

  addKey (key, keyId) {
    const keys = this.keys()
    keys.push(key)
    keys.sort(cmp)
    const i = keys.indexOf(key)
    this._keyIds.splice(i, 0, keyId)
    this.changed = true
    return keys.length <= 2
  }

  addChild (key, keyId, child) {
    let i = 0
    const keys = this.keys()

    for (; i < keys.length; i++) {
      const v = keys[i]
      if (key < v) break
    }

    keys.splice(i, 0, key)
    this._keyIds.splice(i, 0, keyId)
    this.children().splice(i + 1, 0, child)
    this.changed = true

    return keys.length <= 2
  }

  split () {
    const mid = this.keys()[1]
    const midId = this._keyIds[1]
    const right = new FeedNode(this.feed, null, 0, 0, null)

    right._keys = []
    right._keys.push(this._keys.pop())
    right._keyIds = []
    right._keyIds.push(this._keyIds.pop())
    right._children = []
    right.changed = true

    this._keys.pop()
    this._keyIds.pop()

    if (this.children().length) {
      right._children.push(this._children[2])
      right._children.push(this._children[3])
      this._children.pop()
      this._children.pop()
      this.changed = true
    }

    return {
      left: this,
      median: mid,
      medianId: midId,
      right
    }
  }

  child (index) {
    if (this._children) return this._children[index]
    return this.children()[index]
  }

  childCount () {
    return this._children ? this._children.length : this.node.children.length
  }

  children () {
    if (this._children) return this._children
    this._children = []

    for (const [seq, i] of this.node.children) {
      const n = new FeedNode(this.feed, seq ? this.feed[seq].index[i] : this.feed[this.seq].index[i], seq || this.seq, i)
      this._children.push(n)
    }

    return this._children
  }

  keys () {
    if (this._keys) return this._keys
    const keys = this._keys = []
    this._keyIds = []
    for (let seq of this.node.keys) {
      if (!seq) seq = this.seq
      this._keyIds.push(seq)
      keys.push(this.feed[seq].key)
    }
    return keys
  }
}

class Tree {
  constructor (feed = []) {
    this.feed = feed
    this.feed.push(null)
  }

  toString() {
    const root = this.head()
    const m = load(root)

    return require('tree-to-string')(m)

    function load (n) {
      const r = {}
      r.values = n.keys()
      r.children = []
      for (const c of n.children()) {
        r.children.push(load(c))
      }
      return r
    }
  }

  insert (key) {
    const loaded = new Set()
    const index = []
    const stack = []

    let root
    let node = root = this.head()

    if (!node) {
      this.feed.push({
        seq: 1,
        key,
        index: [
          { keys: [0], children: [] }
        ]
      })
      return
    }

    loaded.add(root)

    while (node.childCount()) {
      stack.push(node)

      const keys = node.keys()

      if (key < keys[0]) {
        node = node.child(0)
      } else if (node.childCount() === 2 || key < keys[1]) {
        node = node.child(1)
      } else {
        node = node.child(2)
      }

      loaded.add(node)
    }

    let needsSplit = !node.addKey(key, 0)

    while (needsSplit) {
      const parent = stack.pop()

      if (parent) {
        const { median, medianId, right } = node.split()
        loaded.add(right)
        needsSplit = !parent.addChild(median, medianId, right)
        node = parent
      } else {
        const { median, medianId, right } = node.split()
        root = new FeedNode(this.feed, null, 0, 0)
        root.changed = true
        root._keys = [median]
        root._keyIds = [medianId]
        root._children = [node, right]
        loaded.add(root)
        needsSplit = false
      }
    }

    for (const s of stack) s.changed = true

    indexify(root, index)

    this.feed.push({
      seq: this.feed.length,
      key,
      index
    })
  }

  head () {
    if (this.feed.length === 1) return null
    const h = this.feed[this.feed.length - 1]
    return new FeedNode(this.feed, h.index[0], this.feed.length - 1, 0)
  }
}

function indexify (node, index = []) {
  const i = index.push(null) - 1
  const keys = node._keyIds.slice(0)
  const children = node.children().map((child) => {
    if (!child.changed) return [child.seq, child.index]
    const i = indexify(child, index)
    return [0, i]
  })

  index[i] = { keys, children }
  return i
}

// function insert (val) {
//   if (!feed.length) {
//     feed.push({ index: [], val })
//     return
//   }

//   const root = feed[feed.length - 1]


  // const index = []
  // const stack = []

  // let node = root

  // while (node.children.length) {
  //   stack.push(node)

  //   if (val < node.values[0]) {
  //     node = node.children[0]
  //   } else if (node.children.length === 2 || val < node.values[1]) {
  //     node = node.children[1]
  //   } else {
  //     node = node.children[2]
  //   }
  // }

  // node.values.push(val)
  // node.values.sort(cmp)

  // const inserted = node

  // while (node.values.length > 2) {
  //   const parent = stack.pop()
  //   split(node, parent)
  //   node = parent || root
  // }

  // for (const n of lookup(val)) {
  //   console.log(n, '<-- node')
  // }

  // return node
// }

// function split (node, parent) {
//   const { left, median, right } = splitNode(node)

//   if (!parent) {
//     root = new Node()
//     root.values.push(median)
//     root.children.push(left, right)
//     return [left, root, right]
//   }

//   let i = 0
//   for (; i < parent.values.length; i++) {
//     const v = parent.values[i]
//     if (median < v) break
//   }
//   parent.values.splice(i, 0, median)
//   parent.children.splice(i + 1, 0, right)
//   return [left, parent, right]
// }

function cmp (a, b) {
  return a < b ? -1 : b < a ? 1 : 0
}

const t = new Tree()
let debug = false

require('util').inspect.defaultOptions.depth = Infinity

t.insert('a')
t.insert('b')
t.insert('c')
t.insert('d')

t.insert('e')
t.insert('f')
t.insert('g')
t.insert('h')
t.insert('i')

for (let i = 0; i < 50; i++) {
  t.insert('a' + i)
}

console.log()
console.log(t.toString())
