const test = require('brittle')
const { create, createRange, createStoredCore, createStored, eventFlush } = require('./helpers')
const Hyperbee = require('../index.js')
const SubEncoder = require('sub-encoder')

test('basic getAndWatch append flow', async function (t) {
  const db = create()
  const watcher = await db.getAndWatch('aKey')

  t.is(watcher.node, null)

  await db.put('other', 'key')
  await eventFlush()
  t.is(watcher.node, null)

  await db.put('aKey', 'here')
  await eventFlush()
  const node = watcher.node
  t.is(node.value, 'here')

  await db.put('aKey?', 'not here')
  await eventFlush()
  t.alike(watcher.node, node)

  await db.put('aKey', 'now here')
  await eventFlush()
  t.is(watcher.node.value, 'now here')

  await db.close()
  t.is(watcher.closed, true)
})

test('current value loaded when getAndWatch resolves', async function (t) {
  const db = create()
  await db.put('aKey', 'here')

  const watcher = await db.getAndWatch('aKey')
  t.is(watcher.node.value, 'here')
})

test('throws if bee closing while calling getAndWatch', async function (t) {
  const db = create()
  await db.put('aKey', 'here')

  const prom = db.close()
  await t.exception(db.getAndWatch('aKey'), /Bee closed/)

  await prom
})

test('throws if bee starts closing before getAndWatch resolves', async function (t) {
  const db = create()
  await db.put('aKey', 'here')

  const prom = db.getAndWatch('aKey')
  const closeProm = db.close()
  await t.exception(prom, /Bee closed/)

  await closeProm
})

test('getAndWatch truncate flow', async function (t) {
  const db = create()
  const watcher = await db.getAndWatch('aKey')

  await db.put('aKey', 'here')
  await db.put('otherKey', 'other1Val')
  await db.put('otherKey2', 'otherVal2')
  t.is(db.core.length, 4) // Sanity check

  // Note: Truncate happens before the _onAppend handler was triggered
  // So the onAppend handler will operate on an already-truncated core
  await db.core.truncate(2)

  await eventFlush()
  t.is(watcher.node.value, 'here')

  await db.core.truncate(1)
  await eventFlush()
  t.is(watcher.node, null)

  await db.put('something', 'irrelevant')
  await eventFlush()
  t.is(watcher.node, null)

  await db.put('aKey', 'is back')
  await eventFlush()
  t.is(watcher.node.value, 'is back')
})

test('getAndWatch truncate flow with deletes', async function (t) {
  const db = create()
  const watcher = await db.getAndWatch('aKey')

  await db.put('aKey', 'here')
  await db.put('otherKey', 'other1Val')
  await db.put('otherKey2', 'otherVal2')
  await db.del('aKey')
  t.is(db.core.length, 5) // Sanity check

  await eventFlush()
  t.is(watcher.node, null)

  await db.core.truncate(2)
  await eventFlush()
  t.is(watcher.node.value, 'here')

  await db.core.truncate(1)
  await eventFlush()
  t.is(watcher.node, null)
})

test('getAndWatch emits update', async function (t) {
  t.plan(2)

  const db = create()
  const watcher = await db.getAndWatch('aKey')

  let first = true
  watcher.on('update', () => {
    if (first) {
      t.is(watcher.node.value, 'here')
      first = false
    } else {
      t.is(watcher.node, null)
    }
  })
  await db.put('aKey', 'here')
  await eventFlush()

  await db.core.truncate(1)
  // updates before closing go through
  await watcher.close()

  // After not
  await db.put('aKey', 'back')
  await eventFlush()
})

test('getAndWatch with passed key/value encodings', async function (t) {
  const enc = new SubEncoder()
  const sub = enc.sub('mySub', { keyEncoding: 'utf-8' })

  const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
  const watcher = await db.getAndWatch('entry', { keyEncoding: sub, valueEncoding: 'utf-8' })
  t.is(watcher.node, null)

  await db.put('entry', 'not in sub')
  await eventFlush()
  t.is(watcher.node, null)

  await db.put('entry', 'in sub', { keyEncoding: sub })
  await eventFlush()
  t.is(watcher.node.key, 'entry')
  t.is(watcher.node.value, 'in sub')
})

test('getAndWatch uses the default encodings of the bee', async function (t) {
  const db = create({ keyEncoding: 'utf-8', valueEncoding: 'json' })
  const watcher = await db.getAndWatch('entry')
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

  await watcher.ready()

  eventFlush().then(async () => {
    await db.put('/a.txt')
  })

  for await (const [current, previous] of watcher) { // eslint-disable-line no-unreachable-loop
    t.is(current.version, 2)
    t.is(previous.version, 1)
    break
  }
})

test('basic watch next', async function (t) {
  t.plan(3)

  const db = create()
  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  db.put('/a') // Run on background

  const { done, value: [current, previous] } = await watcher.next()

  t.is(done, false)
  t.is(current.version, 2)
  t.is(previous.version, 1)
})

test('watch multiple next() on parallel - value', async function (t) {
  t.plan(9)

  const db = create()
  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  const a = watcher.next()
  const b = watcher.next()
  const c = watcher.next()

  db.put('/a') // Run on background

  {
    const { done, value: [current, previous] } = await a

    t.is(done, false)
    t.is(current.version, 2)
    t.is(previous.version, 1)
  }

  db.put('/b') // Run on background

  {
    const { done, value: [current, previous] } = await b

    t.is(done, false)
    t.is(current.version, 3)
    t.is(previous.version, 2)
  }

  db.put('/c') // Run on background

  {
    const { done, value: [current, previous] } = await c

    t.is(done, false)
    t.is(current.version, 4)
    t.is(previous.version, 3)
  }
})

test('watch multiple next() on parallel - done', async function (t) {
  t.plan(2)

  const db = create()
  const watcher = db.watch()

  const a = watcher.next()
  const b = watcher.next()

  await watcher.destroy()

  t.alike(await a, { done: true, value: undefined })
  t.alike(await b, { done: true, value: undefined })
})

test('watch next() after is destroyed', async function (t) {
  t.plan(1)

  const db = create()
  const watcher = db.watch()

  await watcher.destroy()

  t.alike(await watcher.next(), { done: true, value: undefined })
})

test('watch waits for new change', async function (t) {
  t.plan(3)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  eventFlush().then(async () => {
    await db.put('/b') // Run on background
  })

  const { done, value: [current, previous] } = await watcher.next()

  t.is(done, false)
  t.is(current.version, 3)
  t.is(previous.version, 2)
})

test('watch does not lose changes if next() was not called yet', async function (t) {
  t.plan(3)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  await db.put('/b')
  await eventFlush()

  await db.put('/c')
  await eventFlush()

  const { done, value: [current, previous] } = await watcher.next()

  t.is(done, false)
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

  t.alike(await watcher.next(), { done: true, value: undefined })
})

test('basic watch on range', async function (t) {
  t.plan(1)

  const db = await createRange(50)

  const watcher = db.watch({ gte: '14' })
  t.teardown(() => watcher.destroy())

  // + could be simpler but could be a helper for other tests
  let next = watcher.next()
  let onchange = null
  next.then(data => {
    next = watcher.next()
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

  watcher.next().then(({ done }) => {
    if (done) {
      t.pass()
      return
    }

    t.fail('should not trigger changes')
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
  watcher.next().then(({ value }) => {
    const [current, previous] = value
    t.is(current.version, 4)
    t.is(previous.version, 3)
  })

  await db.put('/c')
  await eventFlush()

  await db.close()
})

test('destroy watch (without stream)', async function (t) {
  t.plan(3)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.next().then(({ done }) => {
    if (done) {
      t.pass()
      return
    }

    t.fail('should not trigger changes')
  })

  t.absent(watcher.closed)
  await watcher.destroy()
  t.ok(watcher.closed)

  await db.put('/a')
  await eventFlush()
})

test('destroy watch (with stream)', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()

  watcher.next().then(async ({ done }) => {
    if (done) t.fail('should not have been closed')

    t.absent(watcher.closed)
    await watcher.destroy()
    t.ok(watcher.closed)
  })

  await db.put('/a')
})

test('closing bee should destroy watcher', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()

  t.absent(watcher.closed)
  await db.close()
  t.ok(watcher.closed)
})

test('destroy should not trigger stream error', async function (t) {
  t.plan(1)

  const db = create()

  await db.ready()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()

  watcher.next().then(({ done }) => {
    if (done) {
      t.pass()
      return
    }

    t.fail('should not trigger changes')
  }).catch(err => {
    t.fail('should not have given error: ' + err)
  })

  db.core.once('append', async function () {
    await watcher.destroy()
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

  watcher.next().then(() => {
    t.fail('should not trigger changes')
  }).catch(err => {
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

    watcher.next().then(({ value }) => {
      const [current, previous] = value

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

    watcher.next().then(({ done }) => {
      if (!done) changed = true
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
  defaultWatcher.next().then(({ done }) => {
    if (!done) defaultChanged = true
  })

  let allChanged = false
  ignoreAllWatcher.next().then(({ done }) => {
    if (!done) allChanged = true
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
  await watcher.next()
  await flush

  t.pass()

  function differ () {
    return {
      async * [Symbol.asyncIterator] () {
        while (true) {
          if (watcher.closing) throw new Error('Custom stream was destroyed')
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
  await watcher.ready()

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
  await watcher.ready()

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
