/* eslint-disable no-lone-blocks */

const test = require('brittle')
const Hypercore = require('hypercore')
const Hyperbee = require('../index.js')
const createTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const RAM = require('random-access-memory')

test('basic replication', async function (t) {
  const testnet = await createTestnet()

  const swarmA = new Hyperswarm({ bootstrap: testnet.bootstrap })
  const swarmB = new Hyperswarm({ bootstrap: testnet.bootstrap })

  const writer = new Hyperbee(new Hypercore(RAM))
  await writer.ready()

  const reader = new Hyperbee(new Hypercore(RAM, writer.core.key))
  await reader.ready()

  swarmA.on('connection', (socket) => writer.core.replicate(socket))
  const discovery = swarmA.join(writer.core.discoveryKey)
  await discovery.flushed()

  const done = reader.core.findingPeers()
  swarmB.on('connection', (socket) => reader.core.replicate(socket))
  swarmB.join(reader.core.discoveryKey)
  await swarmB.flush().then(done, done) // Awaits just to be already connected to the peer, but issue will still happen in any way at least in the second and third gets

  {
    await writer.put('/a', '1')
    t.ok(await reader.get('/a')) // It doesn't work even though I'm connected to the peer, and "update" option is enabled by default

    t.comment('Force core update')
    await reader.core.update({ wait: true })
    t.ok(await reader.get('/a')) // NOTICE: forcing works here!

    t.comment('Wait a few ms')
    await new Promise(resolve => setTimeout(resolve, 10))
    t.ok(await reader.get('/a')) // Waiting a few ms (at least 2ms) is the only way
  }

  {
    await writer.put('/b', '2')
    t.ok(await reader.get('/b')) // It doesn't work even though I'm connected to the peer, and "update" option is enabled by default

    t.comment('Force core update')
    await reader.core.update({ wait: true })
    t.ok(await reader.get('/b')) // NOTICE: it doesn't work anymore!

    t.comment('Wait a few ms')
    await new Promise(resolve => setTimeout(resolve, 10))
    t.ok(await reader.get('/b')) // Waiting a few ms (at least 2ms) is the only way
  }

  await writer.close()
  await reader.close()

  await swarmA.destroy()
  await swarmB.destroy()

  await testnet.destroy()
})
