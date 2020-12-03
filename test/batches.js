const { create, collect } = require('./helpers')
const tape = require('tape')

tape('basic batch', async function (t) {
  const db = create()

  const b = db.batch()
  await b.put('a', '1')
  await b.put('b', '2')
  await b.flush()

  const all = await collect(db.createReadStream())

  t.same(all, [
    { seq: 1, key: 'a', value: '1' },
    { seq: 2, key: 'b', value: '2' }
  ])

  t.end()
})

tape('batch overwriting itself', async function (t) {
  const db = create()

  const b = db.batch()
  await b.put('a', '1')
  await b.put('a', '2')
  await b.flush()

  const all = await collect(db.createReadStream())

  t.same(all, [
    { seq: 2, key: 'a', value: '2' }
  ])

  t.end()
})

tape('parallel batches', async function (t) {
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

  t.same(all, [
    { seq: 3, key: 'a', value: '3' },
    { seq: 6, key: 'b', value: '6' },
    { seq: 5, key: 'c', value: '5' }
  ])

  t.end()

  async function batch (list) {
    const b = db.batch()

    for (const { type = 'put', key, value } of list) {
      if (type === 'del') await b.del(key)
      else await b.put(key, value)
    }

    return b.flush()
  }
})

tape('batch with sub', async function (t) {
  const db = create()

  {
    const sub = db.sub('sub1')
    const b = sub.batch()
    await b.put('a', '1')
    await b.put('b', '2')
    await b.flush()

    const all = await collect(sub.createReadStream())

    t.same(all, [
      { seq: 1, key: 'a', value: '1' },
      { seq: 2, key: 'b', value: '2' }
    ])
  }

  {
    const sub = db.sub('sub2')
    const b = sub.batch()
    await b.put('a', '1')
    await b.put('b', '2')
    await b.flush()

    const all = await collect(sub.createReadStream())

    t.same(all, [
      { seq: 3, key: 'a', value: '1' },
      { seq: 4, key: 'b', value: '2' }
    ])
  }

  t.end()
})

tape('batch with child batches', async function (t) {
  const db = create()

  const parent = db.batch()

  {
    const sub = db.sub('sub1')
    const b = sub.batch({ batch: parent })
    await b.put('a', '1')
    await b.put('b', '2')

    const all = await collect(b.createReadStream())

    t.same(all, [
      { seq: 1, key: 'a', value: '1' },
      { seq: 2, key: 'b', value: '2' }
    ])
  }

  {
    const sub = db.sub('sub2')
    const b = sub.batch({ batch: parent })
    await b.put('c', '1')
    await b.put('d', '2')

    const all = await collect(b.createReadStream())

    t.same(all, [
      { seq: 3, key: 'c', value: '1' },
      { seq: 4, key: 'd', value: '2' }
    ])
  }

  await parent.flush()

  const all = await collect(db.createReadStream())
  t.same(all.length, 4)

  t.end()
})

tape('batch with child batches and deletion', async function (t) {
  const db = create()

  const parent = db.batch()

  {
    const sub = db.sub('sub1')
    const b = sub.batch({ batch: parent })
    await b.put('a', '1')
    await b.put('b', '2')

    const all = await collect(b.createReadStream())

    t.same(all, [
      { seq: 1, key: 'a', value: '1' },
      { seq: 2, key: 'b', value: '2' }
    ])
  }

  {
    const sub = db.sub('sub2')
    const b = sub.batch({ batch: parent })
    await b.put('c', '1')
    await b.put('d', '2')

    const all = await collect(b.createReadStream())

    t.same(all, [
      { seq: 3, key: 'c', value: '1' },
      { seq: 4, key: 'd', value: '2' }
    ])
  }

  {
    const sub = db.sub('sub2')
    const b = sub.batch({ batch: parent })
    await b.del('c', '1')
    await b.put('e', '3')

    const all = await collect(b.createReadStream())

    t.same(all, [
      { seq: 4, key: 'd', value: '2' },
      { seq: 6, key: 'e', value: '3' }
    ])
  }

  await parent.flush()

  const all = await collect(db.createReadStream())
  t.same(all.length, 4)

  t.end()
})
