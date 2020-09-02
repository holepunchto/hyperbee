const { createRange, collect } = require('./helpers')
const tape = require('tape')

tape('basic history', async function (t) {
  const db = await createRange(10)

  {
    const h = await collect(db.createHistoryStream())
    t.same(h.length, 10)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 1, key: '00' },
      { seq: 2, key: '01' },
      { seq: 3, key: '02' },
      { seq: 4, key: '03' },
      { seq: 5, key: '04' },
      { seq: 6, key: '05' },
      { seq: 7, key: '06' },
      { seq: 8, key: '07' },
      { seq: 9, key: '08' },
      { seq: 10, key: '09' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3 }))
    t.same(h.length, 8)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 3, key: '02' },
      { seq: 4, key: '03' },
      { seq: 5, key: '04' },
      { seq: 6, key: '05' },
      { seq: 7, key: '06' },
      { seq: 8, key: '07' },
      { seq: 9, key: '08' },
      { seq: 10, key: '09' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lte: 5 }))
    t.same(h.length, 3)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 3, key: '02' },
      { seq: 4, key: '03' },
      { seq: 5, key: '04' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lt: 5 }))
    t.same(h.length, 2)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 3, key: '02' },
      { seq: 4, key: '03' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gt: 3, lt: 5 }))
    t.same(h.length, 1)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' }
    ])
  }

  t.end()
})

tape('reverse history', async function (t) {
  const db = await createRange(10)

  {
    const h = await collect(db.createHistoryStream({ reverse: true }))
    t.same(h.length, 10)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 10, key: '09' },
      { seq: 9, key: '08' },
      { seq: 8, key: '07' },
      { seq: 7, key: '06' },
      { seq: 6, key: '05' },
      { seq: 5, key: '04' },
      { seq: 4, key: '03' },
      { seq: 3, key: '02' },
      { seq: 2, key: '01' },
      { seq: 1, key: '00' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, reverse: true }))
    t.same(h.length, 8)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 10, key: '09' },
      { seq: 9, key: '08' },
      { seq: 8, key: '07' },
      { seq: 7, key: '06' },
      { seq: 6, key: '05' },
      { seq: 5, key: '04' },
      { seq: 4, key: '03' },
      { seq: 3, key: '02' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lte: 5, reverse: true }))
    t.same(h.length, 3)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 5, key: '04' },
      { seq: 4, key: '03' },
      { seq: 3, key: '02' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lt: 5, reverse: true }))
    t.same(h.length, 2)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' },
      { seq: 3, key: '02' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gt: 3, lt: 5, reverse: true }))
    t.same(h.length, 1)
    t.same(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' }
    ])
  }

  t.end()
})

tape('live history', async function (t) {
  const db = await createRange(10)

  const s = db.createHistoryStream({ gte: 10, live: true })

  s.once('data', function (data) {
    t.same(data.seq, 10)
    s.once('data', function (data) {
      t.same(data.seq, 11)
      db.feed.close()
      t.end()
    })
    s.on('error', function () {})
    db.put('foo')
  })

  return new Promise(() => {})
})

tape('negative indexes is implicit + version', async function (t) {
  const db = await createRange(10)

  const h = await collect(db.createHistoryStream({ gte: -2 }))

  t.same(h.length, 2)
  t.same(h.map(({ seq, key }) => ({ seq, key })), [
    { seq: 9, key: '08' },
    { seq: 10, key: '09' }
  ])
})

tape('live history can be destroyed', async function (t) {
  const db = await createRange(1)

  let done
  const end = new Promise(resolve => { done = resolve })

  const stream = db.createHistoryStream({ live: true })

  stream.on('data', function () {
    process.nextTick(() => stream.destroy())
  })

  stream.on('close', function () {
    done()
  })

  return end
})
