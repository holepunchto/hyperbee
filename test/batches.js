const { create, createRange, collect } = require('./helpers')
const test = require('brittle')

test('basic batch', async function (t) {
  const db = create()

  const b = db.batch()
  await b.put('a', '1')
  await b.put('b', '2')
  await b.flush()

  const all = await collect(db.createReadStream())

  t.alike(all, [
    { seq: 1, key: 'a', value: '1' },
    { seq: 2, key: 'b', value: '2' }
  ])
})

test('batch peek', async function (t) {
  const db = await createRange(50)

  const e = await db.get('14')
  t.is(e.key, '14')

  const r = await db.peek({ gte: '14' })
  t.alike(r, e)

  const b = db.batch()

  await b.peek({ gte: '14' })
  await b.peek({ gte: '14' })

  await b.flush()
})

test('batch get', async function (t) {
  const db = create()
  await db.put('a', '1')

  const b = db.batch()
  await b.put('b', '2')

  t.alike(await b.get('a'), { seq: 1, key: 'a', value: '1' })
  t.alike(await b.get('b'), { seq: 2, key: 'b', value: '2' })

  await b.flush()
})

test('batch createReadStream', async function (t) {
  const db = create()
  await db.put('a', '1')

  const b = db.batch()
  await b.put('b', '2')

  const allBatch = await collect(b.createReadStream())
  t.alike(allBatch, [
    { seq: 1, key: 'a', value: '1' },
    { seq: 2, key: 'b', value: '2' }
  ])

  const allDb = await collect(db.createReadStream())
  t.alike(allDb, [
    { seq: 1, key: 'a', value: '1' }
  ])

  t.alike(await db.get('a'), { seq: 1, key: 'a', value: '1' })

  await b.flush()

  t.alike(await db.get('a'), { seq: 1, key: 'a', value: '1' })
})

test('batch overwriting itself', async function (t) {
  const db = create()

  const b = db.batch()
  await b.put('a', '1')
  await b.put('a', '2')
  await b.flush()

  const all = await collect(db.createReadStream())

  t.alike(all, [
    { seq: 2, key: 'a', value: '2' }
  ])
})

test('parallel batches', async function (t) {
  const db = create()

  const a = batch([
    { key: 'a', value: '1' },
    { key: 'b', value: '2' }
  ])

  const b = batch([
    { key: 'a', value: '3' },
    { key: 'b', value: '4' },
    { key: 'c', value: '5' }
  ])

  const c = batch([
    { key: 'b', value: '6' }
  ])

  await Promise.all([a, b, c])

  const all = await collect(db.createReadStream())

  t.alike(all, [
    { seq: 3, key: 'a', value: '3' },
    { seq: 6, key: 'b', value: '6' },
    { seq: 5, key: 'c', value: '5' }
  ])

  async function batch (list) {
    const b = db.batch()

    for (const { type = 'put', key, value } of list) {
      if (type === 'del') await b.del(key)
      else await b.put(key, value)
    }

    return b.flush()
  }
})

test('batches can survive parallel ops', async function (t) {
  const db = create()

  const a = db.batch()
  const expected = []
  const p = []

  for (let i = 0; i < 100; i++) {
    const key = 'i-' + i
    const value = key
    expected.push({ seq: 1 + i, key, value })
    p.push(a.put(key, value))
  }

  expected.sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0)

  await Promise.all(p)
  await a.flush()

  const all = await collect(db.createReadStream())
  t.alike(all, expected)
})
