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

test('checkout in the middle of a batch', async function (t) {
  const db = create()
  const batch = db.batch()

  await batch.put('a', 'a')
  await batch.put('b', 'b')
  await batch.flush()

  let checkout = db.checkout(2)

  await t.exception(checkout.get('a'), /Cannot checkout in the middle of a batch/)

  checkout = db.checkout(3)

  t.alike((await checkout.get('a')).value, 'a')
})
