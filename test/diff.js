const test = require('tape')
const { create, createRange, collect, insertRange } = require('./helpers')

test('basic diff', async function (t) {
  const db = await createRange(10)
  const v1 = db.version

  await db.put('a', 'b')

  const diffStream = db.createDiffStream(v1)

  const entries = await collect(diffStream)
  t.same(entries.length, 1)
  t.end()
})

test('bigger diff', async function (t) {
  const db = await createRange(10)
  const v1 = db.version

  await db.del('01')
  await insertRange(db, 9, 12)

  const diffStream = db.createDiffStream(v1)

  const entries = await collect(diffStream)

  t.same(entries.length, 4)
  t.same(entries, [
    { left: null, right: { seq: 2, key: '01', value: null } },
    { left: { seq: 12, key: '09', value: null }, right: { seq: 10, key: '09', value: null } },
    { left: { seq: 13, key: '10', value: null }, right: null },
    { left: { seq: 14, key: '11', value: null }, right: null }
  ])

  t.end()
})

test.skip('diff on multi-level sub db with parent checkout', async function (t) {
  const db = await create()
  const sub = db.sub('hello').sub('world')

  await sub.put('a', 'b')
  await sub.put('b', 'c')

  const v1 = sub.version

  await sub.put('c', 'd')
  await sub.del('a')

  const v2 = sub.version

  await sub.put('e', 'f')
  await sub.put('g', 'h')

  const checkout = db.checkout(v2).sub('hello').sub('world')
  const diffStream = await checkout.createDiffStream(v1)
  const entries = await collect(diffStream)

  console.log('entries:', entries)

  t.end()
})
