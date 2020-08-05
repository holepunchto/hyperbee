const tape = require('tape')
const Hyperbee = require('..')

tape('out of bounds iterator', async function (t) {
  const db = create()

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gt: Buffer.from('c') })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

tape('out of bounds iterator, string encoding', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gte: 'f' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

tape('out of bounds iterator, larger db', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  for (let i = 0; i < 8; i++) {
    await db.put('' + i, 'hello world')
  }

  const s = db.createReadStream({ gte: 'a' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

function create (opts) {
  const feed = require('hypercore')(require('random-access-memory'))
  return new Hyperbee(feed, opts)
}
