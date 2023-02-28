const test = require('brittle')
const { create, createRange, createFromStorage, createTmpDir, eventFlush, sleep } = require('./helpers')

test('basic watch', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function (leftVersion, rightVersion) {
    t.is(leftVersion, 2)
    t.is(rightVersion, 1)
  })

  await db.put('/a')
})

test('basic watch with onchange option on first arg', async function (t) {
  t.plan(1)

  const db = create()
  const onchange = () => t.pass('change')

  const watcher = db.watch(onchange)
  t.teardown(() => watcher.destroy())

  await db.put('/a')
})

test('basic watch with onchange option on second arg', async function (t) {
  t.plan(1)

  const db = create()
  const onchange = () => t.pass('change')

  const watcher = db.watch({}, onchange)
  t.teardown(() => watcher.destroy())

  await db.put('/a')
})

test('basic watch on range', async function (t) {
  t.plan(1)

  const db = await createRange(50)

  const watcher = db.watch({ gte: '14' })
  t.teardown(() => watcher.destroy())

  const onchangefail = () => t.fail('should not trigger changes')
  const onchangepass = () => t.pass('change')

  watcher.on('change', onchangefail)
  await db.put('13')
  await eventFlush()
  await sleep(500)
  watcher.off('change', onchangefail)

  watcher.on('change', onchangepass)
  await db.put('14')
  await eventFlush()
  watcher.off('change', onchangepass)
})

test('batch multiple changes', async function (t) {
  t.plan(2)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function (leftVersion, rightVersion) {
    t.is(leftVersion, 4)
    t.is(rightVersion, 1)
  })

  const batch = db.batch()
  await batch.put('/a')
  await batch.put('/b')
  await batch.put('/c')
  await batch.flush()
})

test('watch a bee with entries already', async function (t) {
  t.plan(3)

  const dir = createTmpDir(t)

  const bee = createFromStorage(dir)
  await bee.put('/a')
  await bee.put('/b')
  await bee.close()

  const db = createFromStorage(dir)
  t.is(db.version, 1)

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.fail('should not trigger changes')
  })

  await db.ready()
  t.is(db.version, 3)

  await eventFlush()
  await sleep(500)

  await db.close()

  t.pass()
})

test('destroy watch', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.fail('should not trigger changes')
  })

  watcher.destroy()

  await db.put('/a')

  await eventFlush()
  await sleep(500)

  t.pass()
})

test('watch and unwatch', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  const onchangefail = () => t.fail('should not trigger changes')
  const onchangepass = () => t.pass('change')

  watcher.unwatch()

  watcher.on('change', onchangefail)
  await db.put('/a')
  await eventFlush()
  await sleep(500)
  watcher.off('change', onchangefail)

  watcher.watch()

  watcher.on('change', onchangepass)
  await db.put('/b')
  await eventFlush()
  watcher.off('change', onchangepass)
})

test('unwatch should not trigger stream error', async function (t) {
  t.plan(3)

  const db = create()

  await db.ready()
  await db.put('/a')

  const watcher = db.watch()
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.fail('should not trigger changes')
  })

  t.absent(watcher.running)

  const put = db.put('/b')

  db.feed.once('append', function () {
    t.ok(watcher.running) // Ensures that stream is created
    watcher.unwatch()
  })

  watcher.on('error', function (err) {
    t.fail('should not have given error: ' + err)
  })

  await put
  await eventFlush()
  await sleep(500)

  t.pass()
})
