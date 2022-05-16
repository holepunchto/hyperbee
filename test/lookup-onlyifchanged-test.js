const tape = require('tape')
const { create } = require('./helpers')

tape('if put does not require lookup, then node has passed key', async t => {
  const SEED = 10
  const db = create()
  const _key = 'key'
  const seen = new Set()
  let lastLen = db.feed.length
  const value = Buffer.from('value')
  let passed = []
  while (lastLen < SEED + 1) { // stop once we've exhausted our key space
    const state = { loop: false, node: null }
    const i = Math.floor(Math.random() * SEED)
    const key = Buffer.from(_key + i)
    await db.put(key, value, {
      onlyIfChanged: true,
      probes: {
        loop () { state.loop = true },
        postloop ({ node }) { state.node = node }
      }
    })

    // if we do not hit the loop condition and the key has been prev inserted then
    // the current node after the lookup loop should have the same key as the
    // key we passed to the put; that is, we should fail to find the node in the
    // lookup iff the node has the block with the key we want to insert.
    if (!state.loop && seen.has(key.toString())) {
      t.equals(Buffer.compare(state.node.block.key, key), 0)
    }
    seen.add(key.toString())
    lastLen = db.feed.length
  }
  t.end()
})
