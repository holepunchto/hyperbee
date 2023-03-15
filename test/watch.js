const test = require('brittle')
const { create, createRange, createStored, eventFlush } = require('./helpers')

test('basic watch', async function (t) {
  t.plan(3)

  const db = create()
  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  db.put('/a') // Run on background

  const { done, value: { current, previous } } = await watcher.next()

  t.is(done, false)
  t.is(current.version, 2)
  t.is(previous.version, 1)
})

test('watch multiple next() on parallel - value', async function (t) {
  t.plan(6)

  const db = create()
  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  const a = watcher.next()
  const b = watcher.next()

  db.put('/b') // Run on background

  {
    const { done, value: { current, previous } } = await a

    t.is(done, false)
    t.is(current.version, 2)
    t.is(previous.version, 1)
  }

  db.put('/c') // Run on background

  {
    const { done, value: { current, previous } } = await b

    t.is(done, false)
    t.is(current.version, 3)
    t.is(previous.version, 2)
  }
})

test('watch multiple next() on parallel - done', async function (t) {
  t.plan(2)

  const db = create()
  const watcher = db.watch()

  const a = watcher.next()
  const b = watcher.next()

  watcher.destroy()

  t.alike(await a, { done: true, value: undefined })
  t.alike(await b, { done: true, value: undefined })
})

test('watch next() after is destroyed', async function (t) {
  t.plan(1)

  const db = create()
  const watcher = db.watch()

  watcher.destroy()

  t.alike(await watcher.next(), { done: true, value: undefined })
})

test('watch waits for new change', async function (t) {
  t.plan(3)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  setImmediate(async () => {
    await eventFlush()
    db.put('/b') // Run on background
  })

  const { done, value: { current, previous } } = await watcher.next()

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

  const { done, value: { current, previous } } = await watcher.next()

  t.is(done, false)
  t.is(current.version, 4)
  t.is(previous.version, 2)
})

test('destroy watch while waiting for a new change', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()

  setImmediate(() => {
    watcher.destroy()
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

  setImmediate(async () => {
    const batch = db.batch()
    await batch.put('/a')
    await batch.put('/b')
    await batch.put('/c')
    await batch.flush()
  })

  for await (const { current, previous } of watcher) { // eslint-disable-line no-unreachable-loop
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
    const { current, previous } = value
    t.is(current.version, 4)
    t.is(previous.version, 3)
  })

  await db.put('/c')
  await eventFlush()

  await db.close()
})

test('destroy watch (without stream)', async function (t) {
  t.plan(4)

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

  watcher.on('close', function () {
    t.pass('watcher closed')
  })

  t.absent(watcher.closed)
  watcher.destroy()
  t.ok(watcher.closed)

  await db.put('/a')
  await eventFlush()
})

test('destroy watch (with stream)', async function (t) {
  t.plan(3)

  const db = create()

  const watcher = db.watch()

  watcher.next().then(({ done }) => {
    if (done) t.fail('should not have been closed')

    watcher.on('close', function () {
      t.pass('watcher closed')
    })

    t.absent(watcher.closed)
    watcher.destroy()
    t.ok(watcher.closed)
  })

  await db.put('/a')
})

test('closing bee should destroy watcher', async function (t) {
  t.plan(3)

  const db = create()

  const watcher = db.watch()

  watcher.on('close', function () {
    t.pass('watcher closed')
  })

  t.absent(watcher.closed)
  await db.close()
  t.ok(watcher.closed)
})

test('destroy should not trigger stream error', async function (t) {
  t.plan(3)

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
  })

  t.absent(watcher.running)

  db.core.once('append', function () {
    t.ok(watcher.running) // Ensures that stream is created
    watcher.destroy()
  })

  watcher.on('error', function (err) {
    t.fail('should not have given error: ' + err)
  })

  await db.put('/b')
  await eventFlush()
})

test('close core in the middle of diffing', async function (t) {
  t.plan(4)

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
  })

  t.absent(watcher.running)

  db.core.prependListener('append', function () {
    t.absent(watcher.running) // Not running yet but about to
    db.core.close()
  })

  watcher.on('error', function (err) {
    t.is(err.code, 'SESSION_CLOSED')
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
      const { current, previous } = value

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

    watcher.destroy()
  }
})
