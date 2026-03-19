const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('hypercore')
const Rache = require('rache')

const Hyperbee = require('../index')

test('entries are not cached using buffers from default slab', async function (t) {
  const dir = await t.tmp()

  const core = new Hypercore(dir)
  const db = new Hyperbee(core)

  await db.put(b4a.from('smallKey'), b4a.from('smallValue'))

  const entry = await db.get('smallKey')

  t.is(entry.key.buffer.byteLength < 100, true, 'Uses a small slab for cached key entry')
  t.is(entry.value.buffer.byteLength < 100, true, 'Uses a small slab for cached value entry')

  await db.close()
})

test('node and key caches are subbed from a passed-in rache', async (t) => {
  const globalCache = new Rache()
  const core = new Hypercore(await t.tmp(), { globalCache })
  const db = new Hyperbee(core)

  t.is(globalCache.globalSize, 0, 'sanity check')
  await db.put('some', 'thing')
  await db.get('some')

  // TODO: for some reason, there's only 1 cache entry
  // total after a put+get, which seems off. Investigate.
  t.is(globalCache.globalSize > 0, true, 'subbed from globalCache')

  await db.close()
})

test('disableCache', async (t) => {
  // Plan assertions for onseq:
  // 2 normal gets, 1 for the 2nd put to get root, 2 get w/ two layers, 1 for put
  const onseqAsserts = 2 + 1 + 2 + 1
  t.plan(onseqAsserts + 4)
  const core = new Hypercore(await t.tmp(), {
    onseq: () => {
      t.pass('called onseq') // Onseq only is called when get is called
    }
  })
  const db = new Hyperbee(core, { disableCache: true })
  await db.ready()

  t.ok(db._disableCache, 'opt sets internal prop')

  await db.put('some', 'thing')
  await db.get('some') // onseq +1
  await db.get('some') // onseq +1

  const preLength = db.core.length // setup for truncation

  await db.put('no', 'thing') // onseq +1
  await t.execution(db.get('foo'), 'ensure getKey() works w/o cache') // onseq +2

  // Because caches are gc'ed when truncating
  await t.execution(db.core.truncate(preLength), 'truncation supports no caches')

  await db.put('new', 'value') // onseq +1

  const prev = db.checkout(preLength + 1)
  t.ok(prev._disableCache, 'internal prop is inherited')

  await prev.close()
  await db.close()
})
