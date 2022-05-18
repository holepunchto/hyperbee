const tape = require('tape')
const { create } = require('./helpers')

tape('bee.cas should put kv pair if comparator evals true, noop otherwise', async t => {
  const key = 'key'
  const value = 'value'

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value + '^', value)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value, value + '^')
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const compare = (x, y) => x === y
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value + '^', value, { compare })
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const compare = (x, y) => x === y
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value, value + '^', { compare })
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, { value: value + '^' }, v)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, v, { value: value + '^' })
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    const compare = (x, y) => JSON.stringify(x) === JSON.stringify(y)
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, { value: value + '^' }, v, { compare })
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    const compare = (x, y) => JSON.stringify(x) === JSON.stringify(y)
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, v, { value: value + '^' }, { compare })
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const snd = await db.cas(k0, Buffer.from(value + '^'), v0)
    t.deepEquals(fst, snd)
  }

  {
    const db = create()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const snd = await db.cas(k0, v0, Buffer.from(value + '^'))
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }
})

tape('bee.batch().cas() should put kv pair if comparator evals true, noop otherwise', async t => {
  const key = 'key'
  const value = 'value'

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value + '^', value)
    await db.flush()
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value, value + '^')
    await db.flush()
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    const compare = (x, y) => x === y
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value + '^', value, { compare })
    await db.flush()
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    const compare = (x, y) => x === y
    await db.put(key, value)
    const fst = await db.get(key)
    const snd = await db.cas(key, value, value + '^', { compare })
    await db.flush()
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, { value: value + '^' }, v)
    await db.flush()
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, v, { value: value + '^' })
    await db.flush()
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    const compare = (x, y) => JSON.stringify(x) === JSON.stringify(y)
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, { value: value + '^' }, v, { compare })
    await db.flush()
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    const compare = (x, y) => JSON.stringify(x) === JSON.stringify(y)
    await db.put(key, v)
    const fst = await db.get(key)
    const snd = await db.cas(key, v, { value: value + '^' }, { compare })
    await db.flush()
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create()
    const db = bee.batch()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const snd = await db.cas(k0, Buffer.from(value + '^'), v0)
    await db.flush()
    t.deepEquals(fst, snd)
  }

  {
    const bee = create()
    const db = bee.batch()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const snd = await db.cas(k0, v0, Buffer.from(value + '^'))
    await db.flush()
    t.equals(snd, null)
    t.notDeepEquals(fst, snd)
  }
})
