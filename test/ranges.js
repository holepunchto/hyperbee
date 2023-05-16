const { createRange } = require('./helpers')
const test = require('brittle')

test('basic peak', async function (t) {
  const db = await createRange(50)

  {
    const e = await db.get('14')
    t.is(e.key, '14')
    const r = await db.peek({ gte: '14' })
    t.alike(r, e)
  }

  {
    const e = await db.get('26')
    t.is(e.key, '26')
    const r = await db.peek({ lt: '27', reverse: true })
    t.alike(r, e)
  }

  {
    const r = await db.peek({ lt: '0', reverse: true })
    t.is(r, null)
  }
})

test('read all', async function (t) {
  const db = await createRange(100)

  let i = 0
  for await (const data of db.createReadStream()) {
    if (i !== Number(data.key)) break
    i++
  }

  t.is(i, 100)
})
