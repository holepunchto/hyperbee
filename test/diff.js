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

test('diff stream on sub + checkout', async function (t) {
  const db = create({ sep: '!', keyEncoding: 'utf-8' })
  const sub = db.sub('sub')

  await db.put('a', 'a')
  await sub.put('sa', 'sa')
  const v1 = sub.version
  await sub.put('sb', 'sb')
  const v2 = sub.version
  await db.put('b', 'b')
  await sub.put('sc', 'sc')

  const entries1 = await collect(sub.checkout(v2).createDiffStream(v1))
  const entries2 = await collect(sub.createDiffStream(v1))

  t.same(entries1.length, 1)
  t.same(entries2.length, 2)
  t.same(entries1[0].left.key, 'sb')
  t.same(entries2[0].left.key, 'sb')
  t.same(entries2[1].left.key, 'sc')

  t.end()
})

test('diff on multi-level sub db with parent checkout', async function (t) {
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

  const c1 = db.checkout(v2).sub('hello').sub('world')
  const c2 = sub.checkout(v2)
  const entries = await collect(c1.createDiffStream(v1))
  const entries2 = await collect(c2.createDiffStream(v1))

  t.same(entries.length, 2)
  t.same(entries2.length, 2)
  t.same(entries[0].right.key, entries2[0].right.key)
  t.same(entries[1].left.key, entries2[1].left.key)

  t.end()
})

test('diff regression', async function (t) {
  const db = await create()

  await db.put('1')
  await db.put('2')
  await db.put('3')
  await db.put('4')
  await db.put('5')
  await db.put('6')
  await db.put('7')
  await db.put('8')
  await db.put('9')

  const v1 = db.version

  await db.del('1')
  await db.del('2')
  await db.del('3')
  await db.del('4')
  await db.del('5')

  const entries = await collect(db.createDiffStream(v1))

  t.same(entries.length, 5)
  t.same(entries, [
    { left: null, right: { seq: 1, key: '1', value: null } },
    { left: null, right: { seq: 2, key: '2', value: null } },
    { left: null, right: { seq: 3, key: '3', value: null } },
    { left: null, right: { seq: 4, key: '4', value: null } },
    { left: null, right: { seq: 5, key: '5', value: null } }
  ])

  t.end()
})
