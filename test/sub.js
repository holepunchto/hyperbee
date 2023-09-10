const test = require('brittle')
const SubEncoder = require('sub-encoder')
const { create } = require('./helpers')

test('create many subs', async function (t) {
  const root = create()
  await root.ready()

  const count = root.core.listenerCount('append')

  for (let i = 0; i < 15; i++) {
    const db = root.sub('things')
    await db.ready()
  }

  t.is(count, root.core.listenerCount('append'))
})

test('sub encoding', async function (t) {
  const db = create()
  const keyEncoding = new SubEncoder('files', 'utf-8')

  await db.put('/a', '1', { keyEncoding })
  t.alike(await db.get('/a', { keyEncoding }), { seq: 1, key: '/a', value: '1' })
})
