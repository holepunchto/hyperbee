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

tape('test all short iterators', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  const SIZE = 8
  const reference = []

  for (let i = 0; i < SIZE; i++) {
    const key = '' + i
    await db.put(key, 'hello world')
    reference.push(key)
  }

  const boundsOpts = [['gte', 'lte'], ['gte', 'lt'], ['gt', 'lte'], ['gt', 'lt']]
  for (let i = 0; i < SIZE; i++) {
    for (let j = 0; j < SIZE; j++) {
      for (const [greater, lesser] of boundsOpts) {
        const opts = { [greater]: '' + i, [lesser]: '' + j }
        const entries = await collect(db.createReadStream(opts))
        if (!validate(opts, entries)) {
          return t.end()
        }
      }
    }
  }

  function validate (opts, entries) {
    const start = opts.gt ? reference.indexOf(opts.gt) + 1 : reference.indexOf(opts.gte)
    const end = opts.lt ? reference.indexOf(opts.lt) : reference.indexOf(opts.lte) + 1
    const range = reference.slice(start, end)
    t.same(range.length, entries.length)
    for (let i = 0; i < range.length; i++) {
      if (!entries[i] || range[i] !== entries[i].key) {
        console.log('FAILED WITH OPTS:', opts)
        console.log('  range length:', range, 'start:', start, 'end:', end)
        console.log('  entries length:', entries.length)
        t.fail(`ranges did not match: expected ${range} got ${entries.map(e => e.key)}`)
        return false
      }
    }
    t.pass('range is ordered correctly')
    return true
  }
})

function collect (stream) {
  return new Promise(resolve => {
    const entries = []
    stream.on('data', d => entries.push(d))
    stream.on('end', () => resolve(entries))
  })
}

function create (opts) {
  const feed = require('hypercore')(require('random-access-memory'))
  return new Hyperbee(feed, opts)
}
