const test = require('brittle')
const b4a = require('b4a')
const Hypercore = require('hypercore')
const makeTmpDir = require('test-tmp')

const Hyperbee = require('../index')

test('entries are not cached using buffers from default slab', async function (t) {
  const dir = await makeTmpDir(t)

  const core = new Hypercore(dir)
  const db = new Hyperbee(core)

  await db.put(b4a.from('smallKey'), b4a.from('smallValue'))

  const entry = await db.get('smallKey')
  console.log(entry.key.buffer.byteLength, entry.value.buffer.byteLength)
  t.is(
    entry.key.buffer.byteLength < 100,
    true,
    'Uses a small slab for cached key entry'
  )
  t.is(
    entry.value.buffer.byteLength < 100,
    true,
    'Uses a small slab for cached value entry'
  )
})
