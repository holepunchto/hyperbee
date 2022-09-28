const { create, collect } = require('./helpers')
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

test.solo('basic batch read ops', async function (t) {
  const db = create()
  await db.put('a', '1')
  await db.put('b', '2')

  const b = db.batch()
  await b.put('c', '3')
  await b.put('d', '4')
  const all = await collect(b.createReadStream())
  t.alike(all, [
    { seq: 1, key: 'a', value: '1' },
    { seq: 2, key: 'b', value: '2' },
    { seq: 3, key: 'c', value: '3' },
    { seq: 4, key: 'd', value: '4' }
  ])
  await b.flush()

  const all2 = await collect(db.createReadStream())
  t.alike(all2, [
    { seq: 1, key: 'a', value: '1' },
    { seq: 2, key: 'b', value: '2' }
  ])
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
