const test = require('brittle')
const { create, createRange, collect, insertRange } = require('./helpers')

test('basic diff', async function (t) {
  const db = await createRange(10)
  const v1 = db.version

  await db.put('a', 'b')

  const diffStream = db.createDiffStream(v1)

  const entries = await collect(diffStream)
  t.is(entries.length, 1)
})

test('basic diff with passed snap', async function (t) {
  const db = await createRange(10)
  const snap = db.snapshot()

  await db.put('a', 'b')

  const nrSessions = db.core.sessions.length
  const diffStream = db.createDiffStream(snap)
  const entries = await collect(diffStream)

  t.is(entries.length, 1)
  t.is(entries[0].right, null) // not yet present in snap
  t.is(snap.closed, false)
  t.is(db.closed, false)
  t.is(nrSessions, db.core.sessions.length) // no leaks
})

test('basic diff with older snap as base', async function (t) {
  const db = await createRange(10)
  const snap = db.snapshot()

  await db.put('a', 'b')

  const nrSessions = db.core.sessions.length
  const diffStream = snap.createDiffStream(db)
  const entries = await collect(diffStream)

  t.is(entries.length, 1)
  t.is(entries[0].left, null) // added in db
  t.is(snap.closed, false)
  t.is(db.closed, false)
  t.is(nrSessions, db.core.sessions.length) // no leaks
})

test('basic diff with 2 snaps', async function (t) {
  const db = await createRange(10)
  const snap = db.snapshot()

  await db.put('a', 'b')
  const newerSnap = db.snapshot()
  await db.put('more', 'stuff')

  const nrSessions = db.core.sessions.length
  const diffStream = snap.createDiffStream(newerSnap)
  const entries = await collect(diffStream)

  t.is(entries.length, 1)
  t.is(entries[0].left, null) // added in newerSnap
  t.is(snap.closed, false)
  t.is(newerSnap.closed, false)
  t.is(nrSessions, db.core.sessions.length) // no leaks
})

test('bigger diff', async function (t) {
  const db = await createRange(10)
  const v1 = db.version

  await db.del('01')
  await insertRange(db, 9, 12)

  const diffStream = db.createDiffStream(v1)

  const entries = await collect(diffStream)

  t.is(entries.length, 4)
  t.alike(entries, [
    { left: null, right: { seq: 2, key: '01', value: null } },
    { left: { seq: 12, key: '09', value: null }, right: { seq: 10, key: '09', value: null } },
    { left: { seq: 13, key: '10', value: null }, right: null },
    { left: { seq: 14, key: '11', value: null }, right: null }
  ])
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

  t.is(entries1.length, 1)
  t.is(entries2.length, 2)
  t.is(entries1[0].left.key, 'sb')
  t.is(entries2[0].left.key, 'sb')
  t.is(entries2[1].left.key, 'sc')
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

  t.is(entries.length, 2)
  t.is(entries2.length, 2)
  t.is(entries[0].right.key, entries2[0].right.key)
  t.is(entries[1].left.key, entries2[1].left.key)
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

  t.is(entries.length, 5)
  t.alike(entries, [
    { left: null, right: { seq: 1, key: '1', value: null } },
    { left: null, right: { seq: 2, key: '2', value: null } },
    { left: null, right: { seq: 3, key: '3', value: null } },
    { left: null, right: { seq: 4, key: '4', value: null } },
    { left: null, right: { seq: 5, key: '5', value: null } }
  ])
})

test('diff key encoding option', async function (t) {
  const db = await create({
    keyEncoding: null
  })
  const v1 = db.version

  await db.put('a', 'b')
  await db.put({ a: 1 }, { b: 2 }, {
    keyEncoding: 'json',
    valueEncoding: 'json'
  })

  const diffStream = db.createDiffStream(v1, {
    gte: { a: 1 },
    keyEncoding: 'json',
    valueEncoding: 'json'
  })

  const entries = await collect(diffStream)
  t.is(entries.length, 1)
})

test('diff oob seek', async function (t) {
  const db = create()

  await db.put('d0')
  await db.put('d1')
  await db.put('d2')
  await db.put('d3')
  await db.put('d4')
  await db.put('d5')
  await db.put('d6')
  await db.put('d7')
  await db.put('d8')
  await db.put('d9')
  await db.del('d9')

  await collect(db.createDiffStream(1, { gt: 'e', lt: 'f' }))
})

test('no session leak after diff stream closes', async function (t) {
  const db = await createRange(5)
  const v1 = db.version

  await db.put('a', 'b')

  const checkout = db.snapshot()

  const nrSessions = db.core.sessions.length
  const diffStream = db.createDiffStream(v1)
  const diffStream2 = checkout.createDiffStream(v1)

  const entries = await collect(diffStream)
  await collect(diffStream2)

  t.is(entries.length, 1) // Sanity check
  t.is(nrSessions, db.core.sessions.length)
})
