const test = require('brittle')
const Hyperbee = require('../')
const Autobase = require('autobase-next')
const Corestore = require('corestore')
const ram = require('random-access-memory')

test.solo('watch works with autobase - basic', async function (t) {
  /* Example output (non-deterministic)
                                      bee appended -- version: 1
                                      bee appended -- version: 2
      printwatch bee1 from 1 to 2
                                      bee appended -- version: 3
      printwatch bee1 from 2 to 3
                                      bee appended -- version: 4
      printwatch bee1 from 3 to 4
                                      bee truncated -- version: 2
                                      bee appended -- version: 3
                                      bee appended -- version: 4
                                      bee appended -- version: 5
                                      bee truncated -- version: 2
      printwatch bee1 from 3 to 4
                                      bee appended -- version: 3
                                      bee appended -- version: 4
                                      bee appended -- version: 5
      printwatch bee1 from 4 to 5
      ok 1 - watch works with autobase - basic # time = 114.423287ms
      /home/hans/holepunch/hyperbee/node_modules/autobase-next/lib/core.js:199
          if (seq >= this.length || seq < 0) throw new Error('Out of bounds get')
    */
  const bases = await createAutobase(2, (...args) => applyForBee(t, ...args), openForBee)
  const [base1, base2] = bases

  // Make base2 writer too
  await base1.append({ add: base2.local.key.toString('hex') })

  await confirm(bases)

  const bee = base1.view // bee based on autobase linearised core

  // For debugging, to illustrate what's going on
  bee.feed.on('truncate', (ancestors, forkId) => console.log('\t\t\t\tbee truncated -- version:', bee.version))
  bee.feed.on('append', () => console.log('\t\t\t\tbee appended -- version:', bee.version))

  printWatch(bee.watch(), 'bee1')

  // Add shared entry
  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm([base1, base2])

  await Promise.all([
    base1.append({ entry: ['1-2', '1-entry2'] }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] })
  ])
  await confirm([base1, base2])
})

async function printWatch (watcher, txt) {
  for await (const { current, previous } of watcher) {
    console.log('printwatch', txt, 'from', previous.version, 'to', current.version)
  }
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
