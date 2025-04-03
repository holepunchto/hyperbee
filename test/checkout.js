const { create, clone } = require('./helpers')
const test = require('brittle')

test('checkouts can point to the future', async function (t) {
  const db = await create(t)

  await db.put('a', 'a')
  await db.put('b', 'b')

  const dbClone = await clone(t, db)

  const s1 = db.core.replicate(true, { keepAlive: false })
  const s2 = dbClone.core.replicate(false, { keepAlive: false })

  s1.pipe(s2).pipe(s1)

  const version = db.version
  await db.put('b', 'b*')

  const checkout = dbClone.checkout(version)

  t.alike((await checkout.get('a')).value, 'a')
  t.alike((await checkout.get('b')).value, 'b')

  await checkout.close()
})

test('checkouts can be set as an option', async function (t) {
  const db = await create(t)

  await db.put('a', 'a')
  await db.put('b', 'b')
  const version = db.version
  await db.put('a', 'a*')

  const v1 = await db.get('a')
  const v2 = await db.get('a', { checkout: version })

  t.is(v1.value, 'a*')
  t.is(v2.value, 'a')

  {
    const expected = ['a*', 'b']
    for await (const data of db.createReadStream()) {
      t.is(data.value, expected.shift())
    }
    t.is(expected.length, 0)
  }

  {
    const expected = ['a', 'b']
    for await (const data of db.createReadStream({ checkout: version })) {
      t.is(data.value, expected.shift())
    }
    t.is(expected.length, 0)
  }
})
