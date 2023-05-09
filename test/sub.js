const test = require('brittle')
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
