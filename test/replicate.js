const test = require('brittle')
const createTestnet = require('@hyperswarm/testnet')
const Hyperswarm = require('hyperswarm')
const Hypercore = require('hypercore')
const RAM = require('random-access-memory')
const Hyperbee = require('../index.js')

test('checkout at remote version not available locally', async function (t) {
  t.plan(1)

  const [swarmA, swarmB] = await useSwarms(t)

  const a = new Hypercore(RAM)
  await a.ready()

  const b = new Hypercore(RAM, a.key)
  await b.ready()

  const db = new Hyperbee(a)
  await db.put('a')
  await db.put('b')

  const clone = new Hyperbee(b)
  await clone.ready()

  swarmA.on('connection', (socket) => db.core.replicate(socket))
  const discovery = await swarmA.join(db.core.discoveryKey, { server: true, client: false })
  await discovery.flushed()

  swarmB.on('connection', (socket) => clone.core.replicate(socket))
  swarmB.join(clone.core.discoveryKey, { server: false, client: true })
  await swarmB.flush()

  // await clone.core.update({ wait: true })
  console.log(clone.core.length)

  const snapshot = clone.checkout(2)

  console.log(await snapshot.get('a'))

  await swarmA.destroy()
  await swarmB.destroy()

  t.pass()
})

async function useSwarms (t) {
  const testnet = await createTestnet(3, { host: '127.0.0.1' })
  t.teardown(() => testnet.destroy())

  const a = new Hyperswarm({ bootstrap: testnet.bootstrap })
  const b = new Hyperswarm({ bootstrap: testnet.bootstrap })

  // t.teardown(() => a.destroy(), 1)
  // t.teardown(() => b.destroy(), 2)

  return [a, b]
}
