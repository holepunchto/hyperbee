const { create, clone } = require('./helpers')
const test = require('brittle')

test('extension set by boolean', async function (t) {
  const db = await create(t)

  await db.put('a', 'a')
  await db.put('b', 'b')

  const dbClone = await clone(t, db, { extension: true })

  const s1 = db.core.replicate(true, { keepAlive: false })
  const s2 = dbClone.core.replicate(false, { keepAlive: false })

  s1.pipe(s2).pipe(s1)

  await new Promise(resolve => dbClone.core.on('append', resolve))

  t.alike((await dbClone.get('a')).value, 'a')
  t.alike((await dbClone.get('b')).value, 'b')
})
