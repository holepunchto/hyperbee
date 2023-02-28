const test = require('brittle')
const { create, createRange, createFromStorage, createTmpDir } = require('./helpers')

// db.watch(prefix, onchange)

test('basic watch', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch('/')
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.pass('change')
  })

  await db.put('/a', Buffer.from('hi'))
})

test('basic watch with onchange option', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch('/', onchange)
  t.teardown(() => watcher.destroy())

  function onchange () {
    t.pass('change')
  }

  await db.put('/a', Buffer.from('hi'))
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
  await sleep(1)
  watcher.off('change', onchangefail)

  watcher.on('change', onchangepass)
  await db.put('14')
  await sleep(1)
  watcher.off('change', onchangepass)
})

test('batch multiple changes', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch('/')
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.pass('change')
  })

  const batch = db.batch()
  await batch.put('/a', Buffer.from('hi'))
  await batch.put('/b', Buffer.from('hi'))
  await batch.put('/c', Buffer.from('hi'))
  await batch.flush()
})

test('watch a bee with entries already', async function (t) {
  t.plan(3)

  const dir = createTmpDir(t)

  const bee = createFromStorage(dir)
  await bee.put('/a', Buffer.from('hi'))
  await bee.put('/b', Buffer.from('hi'))
  await bee.close()

  const db = createFromStorage(dir)
  t.is(db.version, 1)

  const watcher = db.watch('/')
  t.teardown(() => watcher.destroy())

  watcher.on('change', function () {
    t.fail('should not trigger changes')
  })

  await db.ready()
  t.is(db.version, 3)

  await sleep(1)

  await db.close()

  t.pass()
})

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
