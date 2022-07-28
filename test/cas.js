const tape = require('tape')
const { create } = require('./helpers')

tape('bee.put({ cas }) succeds if cas(last, next) returns truthy', async t => {
  const key = 'key'
  const value = 'value'

  {
    const db = create()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value + '^', { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value + '^', { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const cas = (lst, nxt) => JSON.stringify(lst.value) !== JSON.stringify(nxt.value)
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    await db.put(key, { value: value + '^' }, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const cas = (lst, nxt) => JSON.stringify(lst.value) !== JSON.stringify(nxt.value)
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    await db.put(key, { value }, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
    const cas = (lst, nxt) => Buffer.compare(lst.value, nxt.value) !== 0
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    await db.put(k0, Buffer.from(value), { cas })
    const snd = await db.get(k0)
    t.deepEquals(fst, snd)
  }

  {
    const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
    const cas = (lst, nxt) => Buffer.compare(lst.value, nxt.value) !== 0
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    await db.put(k0, Buffer.from(value + '^'), { cas })
    const snd = await db.get(k0)
    t.notDeepEquals(fst, snd)
  }
})

tape('bee.batch().put({ cas }) succeds if cas(last, next) returns truthy', async t => {
  const key = 'key'
  const value = 'value'

  {
    const bee = create()
    const db = bee.batch()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value + '^', { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create()
    const db = bee.batch()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value + '^', { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    const cas = (lst, nxt) => nxt.value !== lst.value
    await db.put(key, value)
    const fst = await db.get(key)
    await db.put(key, value, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const cas = (lst, nxt) => JSON.stringify(lst.value) !== JSON.stringify(nxt.value)
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    await db.put(key, { value: value + '^' }, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const cas = (lst, nxt) => JSON.stringify(lst.value) !== JSON.stringify(nxt.value)
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    await db.put(key, { value }, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'binary' })
    const db = bee.batch()
    const cas = (lst, nxt) => Buffer.compare(lst.value, nxt.value) !== 0
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    await db.put(k0, Buffer.from(value), { cas })
    const snd = await db.get(k0)
    t.deepEquals(fst, snd)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'binary' })
    const db = bee.batch()
    const cas = (lst, nxt) => Buffer.compare(lst.value, nxt.value) !== 0
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    await db.put(k0, Buffer.from(value + '^'), { cas })
    const snd = await db.get(k0)
    t.notDeepEquals(fst, snd)
  }
})

tape('bee.del({ cas }) succeds if cas(last, tomb) returns truthy', async t => {
  const key = 'key'
  const value = 'value'

  {
    const db = create()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value === value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const db = create()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value !== value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value === value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value !== value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) === JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) !== JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) === JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) !== JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const cas = (lst, nxt) => Buffer.compare(lst.value, v0) === 0
    await db.del(key, { cas })
    const snd = await db.get(k0)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const db = create({ keyEncoding: 'binary', valueEncoding: 'binary' })
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const cas = (lst, nxt) => Buffer.compare(lst.value, v0) !== 0
    await db.del(key, { cas })
    const snd = await db.get(k0)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }
})

tape('bee.batch({ cas }) succeds if cas(last, tomb) returns truthy', async t => {
  const key = 'key'
  const value = 'value'

  {
    const bee = create()
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value === value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const bee = create()
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value !== value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value === value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'utf8' })
    const db = bee.batch()
    await db.put(key, value)
    const fst = await db.get(key)
    const cas = (lst) => lst.value !== value
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) === JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) !== JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) === JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'json' })
    const db = bee.batch()
    const v = { value }
    await db.put(key, v)
    const fst = await db.get(key)
    const cas = (lst) => JSON.stringify(lst.value) !== JSON.stringify(v)
    await db.del(key, { cas })
    const snd = await db.get(key)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'binary' })
    const db = bee.batch()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const cas = (lst, nxt) => Buffer.compare(lst.value, v0) === 0
    await db.del(key, { cas })
    const snd = await db.get(k0)
    t.notDeepEquals(fst, snd)
    t.equals(snd, null)
  }

  {
    const bee = create({ keyEncoding: 'utf8', valueEncoding: 'binary' })
    const db = bee.batch()
    const k0 = Buffer.from(key)
    const v0 = Buffer.from(value)
    await db.put(k0, v0)
    const fst = await db.get(k0)
    const cas = (lst, nxt) => Buffer.compare(lst.value, v0) !== 0
    await db.del(key, { cas })
    const snd = await db.get(k0)
    t.deepEquals(fst, snd)
    t.notEquals(snd, null)
  }
})
