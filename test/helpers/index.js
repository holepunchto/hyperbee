const Hyperbee = require('../../')

module.exports = {
  toString,
  create,
  createRange,
  insertRange,
  rangeify,
  collect
}

function collect (stream) {
  return new Promise((resolve, reject) => {
    const entries = []
    stream.on('data', d => entries.push(d))
    stream.on('end', () => resolve(entries))
    stream.on('error', err => reject(err))
    stream.on('close', () => reject(new Error('Premature close')))
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
  return require('tree-to-string')(await load(await tree.getRoot()))

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

function create (opts) {
  opts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8', ...opts }
  const feed = require('hypercore')(require('random-access-memory'))
  return new Hyperbee(feed, opts)
}
