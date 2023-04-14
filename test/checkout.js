const { create, clone } = require('./helpers')
const test = require('brittle')

test('checkouts can point to the future', async function (t) {
  const db = create()

  await db.put('a', 'a')
  await db.put('b', 'b')

  const dbClone = clone(db)

  const s1 = db.core.replicate(true, { keepAlive: false })
  const s2 = dbClone.core.replicate(false, { keepAlive: false })

  s1.pipe(s2).pipe(s1)

  const version = db.version
  await db.put('b', 'b*')

  const checkout = dbClone.checkout(version)

  t.alike((await checkout.get('a')).value, 'a')
  t.alike((await checkout.get('b')).value, 'b')
})
