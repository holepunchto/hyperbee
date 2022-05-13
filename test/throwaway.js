const tape = require('tape')
const { create } = require('./helpers')

tape('batch put onlyIfChanged inserts only if existing value !== value', async t => {
  const db = create()
  const key = Buffer.from('key')
  const value = Buffer.from('value')
  const batch = db.batch()
  await batch.put(key, value)
  const fst = await batch.get(key)
  const fstlen = batch.blocks.size
  await batch.put(key, value, { onlyIfChanged: true })
  const snd = await batch.get(key)
  const sndlen = batch.blocks.size
  await batch.put(key, Buffer.from('va1ue'), { onlyIfChanged: true })
  const thd = await batch.get(key)
  const thdlen = batch.blocks.size
  await batch.flush()
  t.equals(fst.seq, snd.seq)
  t.equals(fstlen, sndlen)
  t.equals(snd.seq, thd.seq - 1)
  t.equals(sndlen, thdlen - 1)
  t.end()
})
