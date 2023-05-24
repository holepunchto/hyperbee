const Hyperbee = require('../../')
const Hypercore = require('hypercore')
const RAM = require('random-access-memory')

module.exports = {
  toString,
  clone,
  create,
  createStoredCore,
  createStored,
  createRange,
  insertRange,
  rangeify,
  collect,
  createCore,
  eventFlush
}

function collect (stream) {
  return new Promise((resolve, reject) => {
    const entries = []
    let ended = false
    stream.on('data', d => entries.push(d))
    stream.on('error', err => reject(err))
    stream.on('end', () => { ended = true })
    stream.on('close', () => {
      if (ended) resolve(entries)
      else reject(new Error('Premature close'))
    })
  })
}

function rangeify (start, end) {
  if (Array.isArray(start)) return start
  if (typeof end !== 'number') {
    end = start
    start = 0
  }

  const r = []
  const l = end.toString().length
  for (; start < end; start++) r.push(start.toString().padStart(l, '0'))
  return r
}

async function insertRange (db, start, end) {
  if (typeof end !== 'number') end = undefined

  const b = db.batch()
  for (const r of rangeify(start, end)) {
    await b.put(r)
  }

  await b.flush()
}

async function createRange (start, end, opts = end) {
  if (typeof end !== 'number') end = undefined

  const db = create(opts)
  await insertRange(db, start, end)
  return db
}

async function toString (tree) {
  return require('tree-to-string')(await load(await tree.getRoot(false)))

  async function load (node) {
    const res = { values: [], children: [] }
    for (let i = 0; i < node.keys.length; i++) {
      res.values.push((await node.getKey(i)).toString())
    }
    for (let i = 0; i < node.children.length; i++) {
      res.children.push(await load(await node.getChildNode(i)))
    }
    return res
  }
}

function clone (db, opts) {
  opts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8', ...opts }
  const clone = new Hypercore(RAM, db.core.key)
  return new Hyperbee(clone, opts)
}

function create (opts) {
  opts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8', ...opts }
  const core = new Hypercore(RAM)
  return new Hyperbee(core, opts)
}

function createStoredCore () {
  const files = new Map()

  return function (...args) {
    return new Hypercore(storage, ...args)
  }

  function storage (name) {
    if (files.has(name)) return files.get(name).clone()
    const st = new RAM()
    files.set(name, st)
    return st
  }
}

function createStored () {
  const create = createStoredCore()

  return function (...args) {
    const core = create(...args)
    return new Hyperbee(core)
  }
}

function createCore () {
  return new Hypercore(require('random-access-memory'))
}

function eventFlush () {
  return new Promise(resolve => setImmediate(resolve))
}
