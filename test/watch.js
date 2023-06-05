const test = require('brittle')
const { create, createRange, createStoredCore, createStored, eventFlush } = require('./helpers')
const Hyperbee = require('../index.js')
const SubEncoder = require('sub-encoder')

test('basic getAndWatch append flow', async function (t) {
  const db = create()
  const watcher = db.getAndWatch('aKey')

  watcher.resume()

  t.is(watcher.current, null)

  await db.put('other', 'key')
  await eventFlush()
  t.is(watcher.current, null)

  await db.put('aKey', 'here')
  await eventFlush()
  const current = watcher.current
  t.is(current.value, 'here')

  await db.put('aKey?', 'not here')
  await eventFlush()
  t.alike(watcher.current, current)

  await db.put('aKey', 'now here')
  await eventFlush()
  t.is(watcher.current.value, 'now here')

  await db.close()
  t.is(watcher.destroying, true)
})

test('current value loaded when getAndWatch resolves', async function (t) {
  const db = create()
  await db.put('aKey', 'here')

  const watcher = db.getAndWatch('aKey', { map: (node) => node.value })
  t.alike(await readOnce(watcher), ['here', null])
})

test('terminates if bee closing while calling getAndWatch', async function (t) {
  t.plan(2)

  const db = create()
  await db.put('aKey', 'here')

  const prom = db.close()
  const stream = db.getAndWatch('aKey')

  stream.resume()

  stream.on('error', function () {
    t.pass('errored')
  })

  stream.on('close', function () {
    t.pass('closed')
  })

  await prom
})

test('terminates if bee starts closing before getAndWatch resolves', async function (t) {
  t.plan(1)

  const db = create()
  await db.put('aKey', 'here')

  const stream = db.getAndWatch('aKey')
  const closeProm = db.close()

  stream.resume()

  stream.on('close', function () {
    t.pass('closed')
  })

  await closeProm
})

test('getAndWatch truncate flow', async function (t) {
  const db = create()
  const watcher = db.getAndWatch('aKey')

  watcher.resume()

  await db.put('aKey', 'here')
  await db.put('otherKey', 'other1Val')
  await db.put('otherKey2', 'otherVal2')
  t.is(db.core.length, 4) // Sanity check

  // Note: Truncate happens before the _onAppend handler was triggered
  // So the onAppend handler will operate on an already-truncated core
  await db.core.truncate(2)

  await eventFlush()
  t.is(watcher.current.value, 'here')

  await db.core.truncate(1)
  await eventFlush()
  t.is(watcher.current, null)

  await db.put('something', 'irrelevant')
  await eventFlush()
  t.is(watcher.current, null)

  await db.put('aKey', 'is back')
  await eventFlush()
  t.is(watcher.current.value, 'is back')
})

test('getAndWatch truncate flow with deletes', async function (t) {
  const db = create()
  const watcher = db.getAndWatch('aKey')

  watcher.resume()

  await db.put('aKey', 'here')
  await db.put('otherKey', 'other1Val')
  await db.put('otherKey2', 'otherVal2')
  await db.del('aKey')
  t.is(db.core.length, 5) // Sanity check

  await eventFlush()
  t.is(watcher.current, null)

  await db.core.truncate(2)
  await eventFlush()
  t.is(watcher.current.value, 'here')

  await db.core.truncate(1)
  await eventFlush()
  t.is(watcher.current, null)
})

test('getAndWatch emits data', async function (t) {
  t.plan(2)

  const db = create()
  const watcher = db.getAndWatch('aKey')

  let first = true
  watcher.on('data', ([current, previous]) => {
    if (first) {
      t.is(current.value, 'here')
      first = false
    } else {
      t.is(current, null)
    }
  })
  await db.put('aKey', 'here')
  await eventFlush()

  await db.core.truncate(1)
  // updates before closing go through
  watcher.destroy()

  // After not
  await db.put('aKey', 'back')
  await eventFlush()
})

test('getAndWatch with passed key/value encodings', async function (t) {
  const enc = new SubEncoder()
  const sub = enc.sub('mySub', { keyEncoding: 'utf-8' })

  const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
  const watcher = db.getAndWatch('entry', { keyEncoding: sub, valueEncoding: 'utf-8' })

  watcher.resume()
  t.is(watcher.node, null)

  await db.put('entry', 'not in sub')
  await eventFlush()
  t.is(watcher.current, null)

  await db.put('entry', 'in sub', { keyEncoding: sub })
  await eventFlush()
  t.is(watcher.current.key, 'entry')
  t.is(watcher.current.value, 'in sub')
})

test('getAndWatch uses the default encodings of the bee', async function (t) {
  const db = create({ keyEncoding: 'utf-8', valueEncoding: 'json' })
  const watcher = db.getAndWatch('entry')

  watcher.resume()
  t.is(watcher.node, null)

  await db.put('entry', { here: 'json' })
  await eventFlush()
  t.is(watcher.node.key, 'entry')
  t.alike(watcher.node.value, { here: 'json' })
})

test('basic watch', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  eventFlush().then(async () => {
    await db.put('/a.txt')
  })

  for await (const [current, previous] of watcher) { // eslint-disable-line no-unreachable-loop
    t.is(current.version, 2)
    t.is(previous.version, 1)
    break
  }
})

test('basic watch read', async function (t) {
  t.plan(2)

  const db = create()
  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  db.put('/a') // Run on background

  const [current, previous] = await readOnce(watcher)

  t.is(current.version, 2)
  t.is(previous.version, 1)
})

test('watch waits for new change', async function (t) {
  t.plan(2)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  eventFlush().then(async () => {
    await db.put('/b') // Run on background
  })

  const [current, previous] = await readOnce(watcher)

  t.is(current.version, 3)
  t.is(previous.version, 2)
})

test('watch does not lose changes if next() was not called yet', async function (t) {
  t.plan(2)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  await db.put('/b')
  await eventFlush()

  await db.put('/c')
  await eventFlush()

  const [current, previous] = await readOnce(watcher)

  t.is(current.version, 4)
  t.is(previous.version, 2)
})

test('destroy watch while waiting for a new change', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()

  eventFlush().then(async () => {
    await watcher.destroy()
  })

  t.alike(await readOnce(watcher), null)
})

test('basic watch on range', async function (t) {
  t.plan(1)

  const db = await createRange(50)

  const watcher = db.watch({ gte: '14' })
  t.teardown(() => watcher.destroy())

  // + could be simpler but could be a helper for other tests
  let next = readOnce(watcher)
  let onchange = null
  next.then(data => {
    next = readOnce(watcher)
    onchange(data)
  })

  onchange = () => t.fail('should not trigger changes')
  await db.put('13')
  await eventFlush()
  onchange = null

  onchange = () => t.pass('change')
  await db.put('14')
  await eventFlush()
  onchange = null
})

test('batch multiple changes', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  eventFlush().then(async () => {
    const batch = db.batch()
    await batch.put('/a')
    await batch.put('/b')
    await batch.put('/c')
    await batch.flush()
  })

  for await (const [current, previous] of watcher) { // eslint-disable-line no-unreachable-loop
    t.is(current.version, 4)
    t.is(previous.version, 1)
    break
  }
})

test('watch ready step should not trigger changes if already had entries', async function (t) {
  t.plan(3)

  const create = createStored()

  const bee = create()
  await bee.put('/a')
  await bee.put('/b')
  await bee.close()

  const db = create()
  t.is(db.version, 1)

  const watcher = db.watch()

  watcher.on('data', function () {
    t.fail('should not trigger changes')
  })
  watcher.on('close', function () {
    t.pass()
  })

  await db.ready()
  t.is(db.version, 3)

  await eventFlush()

  await db.close()
})

test('watch without bee.ready() should trigger the correct version changes', async function (t) {
  t.plan(3)

  const create = createStored()

  const bee = create()
  await bee.put('/a')
  await bee.put('/b')
  await bee.close()

  const db = create()
  t.is(db.version, 1)

  const watcher = db.watch()
  watcher.once('data', function ([current, previous]) {
    t.is(current.version, 4)
    t.is(previous.version, 3)
  })

  await db.put('/c')
  await eventFlush()

  await db.close()
})

test('destroy watch (without stream)', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('data', function () {
    t.fail('should not trigger changes')
  })

  watcher.on('close', function () {
    t.pass()
  })

  watcher.destroy()

  await db.put('/a')
  await eventFlush()
})

test('destroy watch (with stream)', async function (t) {
  const db = create()

  const watcher = db.watch()

  let closed = false
  watcher.on('close', function () {
    closed = true
  })

  await db.put('/a')
  t.absent(closed)
})

test('closing bee should destroy watcher', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()
  watcher.on('close', function () {
    t.pass('watcher closed')
  })

  await db.close()
})

test('destroy should not trigger stream error', async function (t) {
  t.plan(1)

  const db = create()

  await db.ready()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()

  watcher.on('data', function () {
    t.fail('should not trigger changes')
  })

  watcher.on('close', function (closed) {
    t.pass()
  })

  db.core.once('append', function () {
    watcher.destroy()
  })

  await db.put('/b')
  await eventFlush()
})

test('close core in the middle of diffing', async function (t) {
  t.plan(3)

  const createCore = createStoredCore()
  const beeOptions = { keyEncoding: 'utf-8', valueEncoding: 'utf-8' }

  const core = createCore()
  const bee = new Hyperbee(core, beeOptions)
  await bee.put('/a') // Ignore first append (header)
  await bee.close()

  const core2 = createCore()
  core2.on('append', () => core2.close())
  const db = new Hyperbee(core2, beeOptions)

  const watcher = db.watch()

  watcher.on('data', function () {
    t.fail('should not have any data')
  })

  watcher.on('error', function (err) {
    t.is(err.code, 'SESSION_CLOSED')
    t.is(watcher.current, null)
    t.is(watcher.previous, null)
  })

  await db.put('/b')
})

test('create lots of watchers', async function (t) {
  t.plan(1)

  const count = 1000
  const db = create()
  const watchers = []

  for (let i = 0; i < count; i++) {
    const watcher = db.watch()
    t.teardown(() => watcher.destroy())

    watchers.push(watcher)

    watcher.once('data', ([current, previous]) => {
      if (!(current.version === 2 && previous.version === 1)) {
        t.fail('wrong versions')
      }

      if (i === count - 1) {
        t.pass()
      }
    })
  }

  await db.put('/a')
})

test('create and destroy lots of watchers', async function (t) {
  const count = 1000
  const db = create()

  for (let i = 0; i < count; i++) {
    let changed = false

    const watcher = db.watch()

    watcher.once('data', () => {
      changed = true
    })

    await db.put('/a')
    await eventFlush()

    if (!changed) {
      t.fail('should have changed')
    }

    await watcher.destroy()
  }
})

test('can specify own differ', async function (t) {
  const db = create()

  await db.ready()
  await db.put('e1', 'entry1')

  const defaultWatcher = db.watch()
  const ignoreAllWatcher = db.watch(null, {
    differ: () => []
  })

  let defaultChanged = false
  defaultWatcher.once('data', () => {
    defaultChanged = true
  })

  let allChanged = false
  ignoreAllWatcher.once('data', () => {
    allChanged = true
  })

  await db.put('e2', 'entry2')
  await eventFlush()

  t.is(defaultChanged, true)
  t.is(allChanged, false)

  await Promise.all([ignoreAllWatcher.destroy(), defaultWatcher.destroy()])
})

test('slow differ that gets destroyed should not throw', async function (t) {
  t.plan(1)

  const db = create()
  const watcher = db.watch({}, { differ })

  await db.put('/a')
  await eventFlush()

  const flush = eventFlush().then(() => watcher.destroy())
  await readOnce(watcher)
  await flush

  t.pass()

  function differ () {
    return {
      async * [Symbol.asyncIterator] () {
        while (true) {
          if (watcher.destroying) throw new Error('Custom stream was destroyed')
          await eventFlush()
        }
      },
      destroy () {}
    }
  }
})

test('watch with passed key/value encodings', async function (t) {
  const db = create()
  const enc = new SubEncoder()
  const sub = enc.sub('mySub', { keyEncoding: 'utf-8' })

  const watcher = db.watch(sub.range(), { keyEncoding: sub, valueEncoding: 'json' })
  await watcher.opened

  await db.put('not in sub', 'ignored')
  await db.put('in sub 1', { 'this is': 'yielded' }, { keyEncoding: sub, valueEncoding: 'json' })
  await db.put('in sub 2', { 'this is': 'yielded' }, { keyEncoding: sub, valueEncoding: 'json' })

  for await (const [current, previous] of watcher) { // eslint-disable-line no-unreachable-loop
    const diffs = current.createDiffStream(previous, sub.range())
    const entries = []
    for await (const diff of diffs) entries.push(diff.left)

    t.alike(entries.map(e => e.key), ['in sub 1', 'in sub 2'])
    t.alike(entries.map(e => e.value), [{ 'this is': 'yielded' }, { 'this is': 'yielded' }])

    break
  }
})

test('watch uses the bee`s encodings by default', async function (t) {
  const db = create({ keyEncoding: 'utf-8', valueEncoding: 'json' })

  const watcher = db.watch()
  await watcher.opened

  await db.put('entry1', { 'this is': 'entry1' })
  await db.put('entry2', { 'this is': 'entry2' })

  for await (const [current, previous] of watcher) { // eslint-disable-line no-unreachable-loop
    const diffs = current.createDiffStream(previous)
    const entries = []
    for await (const diff of diffs) entries.push(diff.left)

    t.alike(entries.map(e => e.key), ['entry1', 'entry2'])
    t.alike(entries.map(e => e.value), [{ 'this is': 'entry1' }, { 'this is': 'entry2' }])

    break
  }
})

function readOnce (stream) {
  return new Promise((resolve) => {
    const data = stream.read()
    if (data) return resolve(data)

    stream.on('close', onclose)
    stream.once('readable', function () {
      stream.removeListener('close', onclose)
      resolve(stream.read())
    })

    function onclose () {
      resolve(null)
    }
  })
}
