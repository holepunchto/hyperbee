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

test('watch waits for new change', async function (t) {
  t.plan(4)

  const db = create()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  setImmediate(async () => {
    // +
    await eventFlush()
    await new Promise(resolve => setTimeout(resolve, 500))

    db.put('/a') // Run on background
  })

  const started = Date.now()
  const { done, value: { current, previous } } = await watcher.next()

  t.is(done, false)
  t.is(current.version, 3)
  t.is(previous.version, 2)
  t.ok(Date.now() - started >= 500)
})

test('destroy watch while waiting for a new change', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()

  setImmediate(() => {
    watcher.destroy()
  })

  const { value, done } = await watcher.next()
  t.is(done, true)
  t.is(value, undefined)
})

test('basic watch on range', async function (t) {
  t.plan(1)

  const db = await createRange(50)

  const watcher = db.watch({ gte: '14' })
  t.teardown(() => watcher.destroy())

  // + could be simpler but could be a helper for other tests
  let next = watcher.next()
  let onchange = null
  next.then(value => {
    next = watcher.next()
    onchange(value)
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

  for await (const { current, previous } of watcher) {
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

  watcher.next().then(function ({ done }) {
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

test.skip('watch without bee.ready() should trigger the correct version changes', async function (t) {
  t.plan(3)

  const create = createStored()

  const bee = create()
  await bee.put('/a')
  await bee.put('/b')
  await bee.close()

  const db = create()
  t.is(db.version, 1)

  db.watch(function (current, previous) {
    t.is(current.version, 4)
    t.is(previous.version, 3)
  })

  await db.put('/c')
  await eventFlush()

  await db.close()
})

test.skip('destroy watch (without stream)', async function (t) {
  t.plan(3)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
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

test.skip('destroy watch (with stream)', async function (t) {
  t.plan(3)

  const db = create()

  const watcher = db.watch()

  watcher.on('change', function () {
    watcher.on('close', function () {
      t.pass('watcher closed')
    })

    t.absent(watcher.closed)
    watcher.destroy()
    t.ok(watcher.closed)
  })

  await db.put('/a')
})

test.skip('closing bee should destroy watcher', async function (t) {
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

test.skip('destroy should not trigger stream error', async function (t) {
  t.plan(2)

  const db = create()

  await db.ready()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch(function () {
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

test.skip('close core in the middle of diffing', async function (t) {
  t.plan(3)

  const db = create()

  await db.ready()
  await db.put('/a') // Ignore first append (header)

  const watcher = db.watch(function () {
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

test.skip('create lots of watchers', async function (t) {
  t.plan(1)

  const count = 1000
  const db = create()
  const watchers = []

  for (let i = 0; i < count; i++) {
    const watcher = db.watch()
    t.teardown(() => watcher.destroy())

    watchers.push(watcher)

    watcher.on('change', function (current, previous) {
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

test.skip('create and destroy lots of watchers', async function (t) {
  const count = 1000
  const db = create()

  for (let i = 0; i < count; i++) {
    let changed = false

    const watcher = db.watch(function () {
      changed = true
    })

    await db.put('/a')
    await eventFlush()

    if (!changed) {
      t.fail('should have changed')
    }

    watcher.destroy()
  }
})
