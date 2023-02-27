const test = require('brittle')
const { create, createFromStorage, createTmpDir } = require('./helpers')
const RAM = require('random-access-memory')

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

test.skip('basic watch on prefix', async function (t) {
  t.plan(1)

  const db = create()

  const watcher = db.watch('/sub')
  t.teardown(() => watcher.destroy())

  {
    watcher.on('change', onchangefail)

    await db.put('/a', Buffer.from('hi'))
    await sleep(1)

    watcher.off('change', onchangefail)
  }

  {
    watcher.on('change', onchangepass)

    await db.put('/sub/b', Buffer.from('hi'))
    await sleep(1)

    watcher.off('change', onchangepass)
  }

  function onchangefail () {
    t.fail('should not trigger changes')
  }

  function onchangepass () {
    t.pass('change')
  }
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

  t.pass()
})

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
