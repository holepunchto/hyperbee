const { createRange } = require('./helpers')
const tape = require('tape')

tape('basic peak', async function (t) {
  const db = await createRange(50)

  {
    const e = await db.get('14')
    t.same(e.key, '14')
    const r = await db.peek({ gte: '14' })
    t.same(r, e)
  }

  {
    const e = await db.get('26')
    t.same(e.key, '26')
    const r = await db.peek({ lt: '27', reverse: true })
    t.same(r, e)
  }

  {
    const r = await db.peek({ lt: '0', reverse: true })
    t.same(r, null)
  }

  t.end()
})
