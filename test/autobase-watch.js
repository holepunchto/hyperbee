const test = require('brittle')
const Hyperbee = require('../')
const Autobase = require('autobase-next')
const Corestore = require('corestore')
const ram = require('random-access-memory')
const { collect } = require('./helpers')

test('watch works with autobase - basic', async function (t) {
  const bases = await createAutobase(2, (...args) => applyForBee(t, ...args), openForBee)
  const [base1, base2] = bases

  // Make base2 writer too
  await base1.append({ add: base2.local.key.toString('hex') })
  await confirm(bases)

  const bee = base1.view // bee based on autobase linearised core

  const partialWatcher = bee.watch()
  const fullWatcher = bee.watch()
  const initBee = bee.snapshot()

  // Start consuming the watchers
  const consumePartialWatcherProm = consumeWatcher(partialWatcher)
  const consumeFullWatcherProm = consumeWatcher(fullWatcher)

  // Add shared entry
  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm([base1, base2])

  await partialWatcher.destroy()
  const partialDiffs = await consumePartialWatcherProm

  // Init state
  t.alike(initBee.version, partialDiffs[0].previous.version)
  // Final state
  const partialFinal = await collect(partialDiffs[partialDiffs.length - 1].current.createReadStream())
  t.alike(partialFinal.length, 1) // Sanity check
  t.alike(partialFinal, await collect(bee.createReadStream()))

  await Promise.all([
    base1.append({ entry: ['1-2', '1-entry2'] }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] })
  ])
  await confirm([base1, base2])

  await fullWatcher.destroy()
  const fullDiffs = await consumeFullWatcherProm

  // sanity check. Even though the exact amount is non-deterministic
  // it should have been triggered at least a few times.
  t.is(fullDiffs.length > 1, true)
  t.alike(
    await collect(fullDiffs[0].previous.createReadStream()),
    await collect(initBee.createReadStream())
  )
  // Final state
  const finalEntries = await collect(fullDiffs[fullDiffs.length - 1].current.createReadStream())
  t.is(finalEntries.length, 4) // Sanity check
  t.alike(finalEntries, await collect(bee.createReadStream()))
})

async function consumeWatcher (watcher) {
  const entries = []
  for await (const { current, previous } of watcher) {
    entries.push({ previous, current })
  }
  return entries
}

async function createAutobase (n, apply, open) {
  const opts = { apply, open, valueEncoding: 'json' }
  const bases = [new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(0) }), null, opts)]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(i) }), bases[0].local.key, opts)
    await base.ready()
    bases.push(base)
  }
  return bases
}

async function applyForBee (t, batch, view, base) {
  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    } else {
      try {
        await view.put(...value.entry, { update: false })
      } catch (e) {
        console.error(e)
        t.fail()
      }
    }
  }
}

function openForBee (linStore) {
  const beeOpts = { extension: false, keyEncoding: 'binary', valueEncoding: 'binary' }

  const core = linStore.get('simple-bee', { valueEncoding: 'binary' })
  const view = new Hyperbee(core, beeOpts)
  return view
}

async function confirm (bases) {
  await sync(bases)

  const writers = bases.filter(b => !!b.localWriter)
  const maj = Math.floor(writers.length / 2) + 1

  for (let i = 0; i < maj; i++) await writers[i].append(null)
  await sync(bases)
  for (let i = 0; i < maj; i++) await writers[i].append(null)
  return sync(bases)
}

async function sync (bases) {
  const streams = []
  const missing = bases.slice()

  while (missing.length) {
    const a = missing.pop()

    for (const b of missing) {
      const s1 = a.store.replicate(true)
      const s2 = b.store.replicate(false)

      s1.on('error', () => {})
      s2.on('error', () => {})

      s1.pipe(s2).pipe(s1)

      streams.push(s1)
      streams.push(s2)
    }
  }

  await Promise.all(bases.map(b => b.update({ wait: true })))

  const closes = []

  for (const stream of streams) {
    stream.destroy()
    closes.push(new Promise(resolve => stream.on('close', resolve)))
  }

  await Promise.all(closes)
}
