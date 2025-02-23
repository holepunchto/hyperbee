const Hyperbee = require('../../')
const Hypercore = require('hypercore')

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

async function createRange (t, start, end, opts = end) {
  if (typeof end !== 'number') end = undefined

  const db = await create(t, opts)
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

async function clone (t, db, opts) {
  opts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8', ...opts }
  const storage = await t.tmp()
  const clone = new Hypercore(storage, db.core.key)
  const cdb = new Hyperbee(clone, opts)
  t.teardown(() => cdb.close())
  return cdb
}

async function create (t, opts) {
  opts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8', ...opts }
  const storage = await t.tmp()
  const core = new Hypercore(storage)
  const db = new Hyperbee(core, opts)
  t.teardown(() => db.close())
  return db
}

async function createStoredCore (t) {
  const storage = await t.tmp()
  return function (...args) {
    const core = new Hypercore(storage, ...args)
    t.teardown(() => core.close())
    return core
  }
}

async function createStored (t) {
  const create = await createStoredCore(t)

  return function (...args) {
    const core = create(...args)
    const db = new Hyperbee(core)
    t.teardown(() => db.close())
    return db
  }
}

async function createCore (t) {
  const core = new Hypercore(await t.tmp())
  t.teardown(() => core.close())
  return core
}

function eventFlush () {
  return new Promise(resolve => setTimeout(resolve, 1000))
}
