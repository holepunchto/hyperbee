# Hyperbee 🐝

[See API docs at docs.holepunch.to](https://docs.holepunch.to/building-blocks/hyperbee)

An append-only B-tree running on a Hypercore. Allows sorted iteration and more.

```
npm install hyperbee
```

## Usage

```js
const Hyperbee = require('hyperbee')
const Hypercore = require('hypercore')
const RAM = require('random-access-memory')

const core = new Hypercore(RAM)
const db = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'binary' })

// If you own the core
await db.put('key1', 'value1')
await db.put('key2', 'value2')
await db.del('some-key')

// If you want to insert/delete batched values
const batch = db.batch()

await batch.put('key', 'value')
await batch.del('some-key')
await batch.flush() // Execute the batch

// Query the core
const entry = await db.get('key') // => null or { key, value }

// Read all entries
for await (const entry of db.createReadStream()) {
  // ..
}

// Read a range
for await (const entry of db.createReadStream({ gte: 'a', lt: 'd' })) {
  // Anything >=a and <d
}

// Get the last written entry
for await (const entry of db.createHistoryStream({ reverse: true, limit: 1 })) {
  // ..
}
```

It works with sparse cores, only a small subset of the full core is downloaded to satisfy your queries.

## API

#### `const db = new Hyperbee(core, [options])`

Make a new Hyperbee instance. `core` should be a [Hypercore](https://github.com/holepunchto/hypercore).

`options` include:
```js
{
  keyEncoding: 'binary', // "binary" (default), "utf-8", "ascii", "json", or an abstract-encoding
  valueEncoding: 'binary' // Same options as keyEncoding like "json", etc
}
```

Note that currently read/diff streams sort based on the encoded value of the keys.

#### `await db.ready()`

Waits until internal state is loaded.

Use it once before reading synchronous properties like `db.version`, unless you called any of the other APIs.

#### `await db.close()`

Fully close this bee, including its core.

#### `db.core`

The underlying Hypercore backing this bee.

#### `db.version`

Number that indicates how many modifications were made, useful as a version identifier.

#### `db.id`

String containing the id (z-base-32 of the public key) identifying this bee.

#### `db.key`

Buffer containing the public key identifying this bee.

#### `db.discoveryKey`

Buffer containing a key derived from `db.key`.

This discovery key does not allow you to verify the data, it's only to announce or look for peers that are sharing the same bee, without leaking the bee key.

#### `db.writable`

Boolean indicating if we can put or delete data in this bee.

#### `db.readable`

Boolean indicating if we can read from this bee. After closing the bee this will be `false`.

#### `await db.put(key, [value], [options])`

Insert a new key. Value can be optional.

If you're inserting a series of data atomically or want more performance then check the `db.batch` API.

`options` includes:
```js
{
  cas (prev, next) { return true }
}
```

##### Compare And Swap (cas)
`cas` option is a function comparator to control whether the `put` succeeds.

By returning `true` it will insert the value, otherwise it won't.

It receives two args: `prev` is the current node entry, and `next` is the potential new node.

```js
await db.put('number', '123', { cas })
console.log(await db.get('number')) // => { seq: 1, key: 'number', value: '123' }

await db.put('number', '123', { cas })
console.log(await db.get('number')) // => { seq: 1, key: 'number', value: '123' }
// Without cas this would have been { seq: 2, ... }, and the next { seq: 3 }

await db.put('number', '456', { cas })
console.log(await db.get('number')) // => { seq: 2, key: 'number', value: '456' }

function cas (prev, next) {
  // You can use same-data or same-object lib, depending on the value complexity
  return prev.value !== next.value
}
```

#### `const { seq, key, value } = await db.get(key)`

Get a key's value. Returns `null` if key doesn't exists.

`seq` is the Hypercore index at which this key was inserted.

#### `await db.del(key, [options])`

Delete a key.

`options` include:
```js
{
  cas (prev, next) { return true }
}
```

##### Compare And Swap (cas)
`cas` option is a function comparator to control whether the `del` succeeds.

By returning `true` it will delete the value, otherwise it won't.

It only receives one arg: `prev` which is the current node entry.

```js
// This won't get deleted
await db.del('number', { cas })
console.log(await db.get('number')) // => { seq: 1, key: 'number', value: 'value' }

// Change the value so the next time we try to delete it then "cas" will return true
await db.put('number', 'can-be-deleted')

await db.del('number', { cas })
console.log(await db.get('number')) // => null

function cas (prev) {
  return prev.value === 'can-be-deleted'
}
```

#### `const { key, value } = await db.getBySeq(seq, [options])`

Get the key and value from a block number.

`seq` is the Hypercore index. Returns `null` if block doesn't exists.

#### `const stream = db.replicate(isInitiatorOrStream)`

See more about how replicate works at [core.replicate][core-replicate-docs].

#### `const batch = db.batch()`

Make a new atomic batch that is either fully processed or not processed at all.

If you have several inserts and deletions then a batch can be much faster.

#### `await batch.put(key, [value], [options])`

Insert a key into a batch.

`options` are the same as `db.put` method.

#### `const { seq, key, value } = await batch.get(key)`

Get a key, value out of a batch.

#### `await batch.del(key, [options])`

Delete a key into the batch.

`options` are the same as `db.del` method.

#### `await batch.flush()`

Commit the batch to the database, and releases any locks it has acquired.

#### `await batch.close()`

Destroy a batch, and releases any locks it has acquired on the db.

Call this if you want to abort a batch without flushing it.

#### `const stream = db.createReadStream([range], [options])`

Make a read stream. Sort order is based on the binary value of the keys.

All entries in the stream are similar to the ones returned from `db.get`.

`range` should specify the range you want to read and looks like this:

```js
{
  gt: 'only return keys > than this',
  gte: 'only return keys >= than this',
  lt: 'only return keys < than this',
  lte: 'only return keys <= than this'
}
```

`options` include:

```js
{
  reverse: false // Set to true to get them in reverse order,
  limit: -1 // Set to the max number of entries you want
}
```

#### `const { seq, key, value } = await db.peek([range], [options])`

Similar to doing a read stream and returning the first value, but a bit faster than that.

#### `const stream = db.createHistoryStream([options])`

Create a stream of all entries ever inserted or deleted from the db.

Each entry has an additional `type` property indicating if it was a `put` or `del` operation.

`options` include:
```js
{
  live: false, // If true the stream will wait for new data and never end
  reverse: false, // If true get from the newest to the oldest
  gte: seq, // Start with this seq (inclusive)
  gt: seq, // Start after this index
  lte: seq, // Stop after this index
  lt: seq, // Stop before this index
  limit: -1 // Set to the max number of entries you want
}
```

If any of the `gte`, `gt`, `lte`, `lt` arguments are `< 0` then
they'll implicitly be added with the version before starting so
doing `{ gte: -1 }` makes a stream starting at the last index.

#### `const stream = db.createDiffStream(otherVersion, [options])`

Efficiently create a stream of the shallow changes between two versions of the db.

`options` are the same as `db.createReadStream`, except for `reverse`.

Each entry is sorted by key and looks like this:
```js
{
  left: Object, // The entry in the `db`
  right: Object // The entry in `otherVersion`
}
```

If an entry exists in db but not in the other version, then `left` is set
and `right` will be null, and vice versa.

If the entries are causally equal (i.e. the have the same seq), they are not
returned, only the diff.

#### `const entryWatcher = await db.getAndWatch(key, [options])`

Returns a watcher which listens to changes on the given key.

`entryWatcher.node` contains the current entry in the same format as the result of `bee.get(key)`,  and will be updated as it changes.

By default, the node will have the bee's key- and value encoding, but you can overwrite it by setting the `keyEncoding` and `valueEncoding` options.

You can listen to `entryWatcher.on('update')` to be notified when the value of node has changed.

Call `await watcher.close()` to stop the watcher.

#### `const watcher = db.watch([range])`

Listens to changes that are on the optional `range`.

`range` options are the same as `db.createReadStream` except for `reverse`.

By default, the yielded snapshots will have the bee's key- and value encoding, but you can overwrite them by setting the `keyEncoding` and `valueEncoding` options.

Usage example:
```js
for await (const [current, previous] of watcher) {
  console.log(current.version)
  console.log(previous.version)
}
```

Returns a new value after a change, `current` and `previous` are snapshots that are auto-closed before next value.

Don't close those snapshots yourself because they're used internally, let them be auto-closed.

Watchers on subs and checkouts are not supported. Instead, use the range option to limit scope.

`await watcher.ready()`

Waits until the watcher is loaded and detecting changes.

`await watcher.close()`

Stops the watcher. You could also stop it by using `break` in the loop.

#### `const snapshot = db.checkout(version)`

Get a read-only snapshot of a previous version.

#### `const snapshot = db.snapshot()`

Shorthand for getting a checkout for the current version.

#### `const sub = db.sub('sub-prefix', options = {})`

Create a sub-database where all entries will be prefixed by a given value.

This makes it easy to create namespaces within a single Hyperbee.

`options` include:
```js
{
  sep: Buffer.alloc(1), // A namespace separator
  valueEncoding, // Optional sub valueEncoding (defaults to the parents)
  keyEncoding // Optional sub keyEncoding (defaults to the parents)
}
```

For example:
```js
const root = new Hyperbee(core)
const sub = root.sub('a')

// In root, this will have the key ('a' + separator + 'b')
await sub.put('b', 'hello')

// Returns => { key: 'b', value: 'hello')
await sub.get('b')
```

#### `const header = await db.getHeader([options])`

Returns the header contained in the first block. Throws if undecodable.

`options` are the same as the `core.get` method.

#### `const isHyperbee = await Hyperbee.isHyperbee(core, [options])`

Returns `true` if the core contains a Hyperbee, `false` otherwise.

This requests the first block on the core, so it can throw depending on the options.

`options` are the same as the `core.get` method.

[core-replicate-docs]: https://github.com/holepunchto/hypercore#const-stream--corereplicateisinitiatororreplicationstream
