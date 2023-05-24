const { createRange, collect } = require('./helpers')
const test = require('brittle')

test('basic history', async function (t) {
  const db = await createRange(10)

  {
    const h = await collect(db.createHistoryStream())
    t.is(h.length, 10)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
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
    t.is(h.length, 8)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
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
    t.is(h.length, 3)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 3, key: '02' },
      { seq: 4, key: '03' },
      { seq: 5, key: '04' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lt: 5 }))
    t.is(h.length, 2)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 3, key: '02' },
      { seq: 4, key: '03' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gt: 3, lt: 5 }))
    t.is(h.length, 1)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' }
    ])
  }
})

test('reverse history', async function (t) {
  const db = await createRange(10)

  {
    const h = await collect(db.createHistoryStream({ reverse: true }))
    t.is(h.length, 10)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
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
    t.is(h.length, 8)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
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
    t.is(h.length, 3)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 5, key: '04' },
      { seq: 4, key: '03' },
      { seq: 3, key: '02' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gte: 3, lt: 5, reverse: true }))
    t.is(h.length, 2)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' },
      { seq: 3, key: '02' }
    ])
  }

  {
    const h = await collect(db.createHistoryStream({ gt: 3, lt: 5, reverse: true }))
    t.is(h.length, 1)
    t.alike(h.map(({ seq, key }) => ({ seq, key })), [
      { seq: 4, key: '03' }
    ])
  }
})

test('live history', async function (t) {
  t.plan(2)

  const db = await createRange(10)

  const s = db.createHistoryStream({ gte: 10, live: true })

  s.once('data', function (data) {
    t.is(data.seq, 10)
    s.once('data', function (data) {
      t.is(data.seq, 11)
      db.core.close()
    })
    s.on('error', function () {})
    db.put('foo')
  })
})

test('negative indexes is implicit + version', async function (t) {
  const db = await createRange(10)

  const h = await collect(db.createHistoryStream({ gte: -2 }))

  t.is(h.length, 2)
  t.alike(h.map(({ seq, key }) => ({ seq, key })), [
    { seq: 9, key: '08' },
    { seq: 10, key: '09' }
  ])
})

test('live history can be destroyed', async function (t) {
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

test('no session leak after history stream closes', async function (t) {
  const db = await createRange(5)
  const v1 = db.version

  const snap = db.snapshot()
  const nrSessions = db.core.sessions.length
  const stream = db.createHistoryStream(v1)
  const stream2 = snap.createHistoryStream(v1)

  const entries = await collect(stream)
  await collect(stream2)

  t.is(entries.length, 5) // Sanity check
  t.is(nrSessions, db.core.sessions.length)
})
