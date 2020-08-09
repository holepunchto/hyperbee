# Hyperbee 🐝

An append-only Btree running on a Hypercore.
Allows sorted iteration and more.

```
npm install hyperbee
```

## Usage

``` js
const Hyperbee = require('hyperbee')
const db = new Hyperbee(feed)

// if you own the feed
await db.put('key', 'value')
await db.del('some-key')

// if you want to insert/delete batched values
const batch = db.batch()

await batch.put('key', 'value')
await batch.del('some-key')
await batch.flush() // execute the batch

// if you want to query the feed
const node = await db.get('key') // null or { key, value }

// if you want to read a range
const rs = db.createReadStream({ gt: 'a', lt: 'd' }) // anything >a and <d
const rs = db.createReadStream({ gte: 'a', lte: 'd' }) // anything >=a and <=d
```

Some of the internals are still being tweaked but overall the API and feature set is pretty
stable if you want to try it out.
