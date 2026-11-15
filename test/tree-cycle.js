const test = require('brittle')
const b4a = require('b4a')
const { create, collect } = require('./helpers')
const { Node, YoloIndex } = require('../lib/messages')

// #206 — a child pointer that points at its own tree node used to spin
// forever in Batch._get (already-resolved awaits starve the event loop, so
// even `timeout` never fires).

async function appendSelfChildRoot(core) {
  const seq = core.length
  const index = YoloIndex.encode({
    levels: [{ keys: [seq], children: [seq, 0, seq, 0] }]
  })
  await core.append(Node.encode({ index, key: b4a.from('m'), value: null }))
}

test('get throws on a self-pointing child instead of hanging (#206)', async function (t) {
  const db = await create(t)
  await db.put('a', '1')
  await appendSelfChildRoot(db.core)

  const t0 = Date.now()
  await t.exception(db.get('z'), /cycle in child pointers/)
  // Unbounded descent never returns; the depth bound must fail fast.
  t.ok(Date.now() - t0 < 1000, 'get returned instead of hanging')
})

test('range open throws on a self-pointing child (#206)', async function (t) {
  const db = await create(t)
  await db.put('a', '1')
  await appendSelfChildRoot(db.core)

  await t.exception(collect(db.createReadStream({ gte: 'z' })), /cycle in child pointers/)
})

test('healthy trees still get and iterate (#206)', async function (t) {
  const db = await create(t)
  await db.put('a', '1')
  await db.put('b', '2')
  await db.put('c', '3')

  const entry = await db.get('b')
  t.is(entry.key, 'b')
  t.is(entry.value, '2')

  const all = await collect(db.createReadStream())
  t.is(all.length, 3)
})
