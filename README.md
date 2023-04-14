# Hyperbee 🐝

[See API docs at docs.holepunch.to](https://docs.holepunch.to/building-blocks/hyperbee)

An append-only B-tree running on a Hypercore.
Allows sorted iteration and more.

```
npm install hyperbee
```

## Usage

``` js
const Hyperbee = require('hyperbee')
const db = new Hyperbee(core, {
  keyEncoding: 'utf-8', // can be set to undefined (binary), utf-8, ascii or and abstract-encoding
  valueEncoding: 'binary' // same options as above
})

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

// get the last written entry
const rs = db.createHistoryStream({ reverse: true, limit: 1 })
```

Some of the internals are still being tweaked, but overall the API and feature set is pretty
stable, if you want to try it out.

All of the above methods work with sparse feeds, meaning only a small subset of the full
feed is downloaded to satisfy your queries.

## API

#### `const db = new Hyperbee(core, [options])`

Make a new Hyperbee instance. `feed` should be a [Hypercore](https://github.com/holepunchto/hypercore).

Options include:

```
{
  keyEncoding: 'utf-8' | 'binary' | 'ascii', // or some abstract encoding
  valueEncoding: <same as above>
}
```

Note that currently read/diff streams sort based on the *encoded* value of the keys.

#### `await db.put(key, [value], [options])`

Insert a new key. Value can be optional. If you are inserting a series of data atomically,
or you just have a batch of inserts/deletions available using a batch can be much faster
than simply using a series of puts/dels on the db.

Options include:

```js
{
  cas (prev, next) { return true }
}
```

##### Compare And Swap (cas)
You have the option to pass a `cas` function as an option to `put` that controls whether the `put` succeeds. Given `bee.put(key, value, { cas })`, `cas` is passed the current node (i.e. `{ seq, key, value }`) in `bee` at `key` and the next _tentative_ node. Then `put` succeeds only if `cas` returns `true` and fails otherwise.

```js
const cas = (prev, next) => prev.value !== next.value
const db = new Hyperbee(core, { keyEncoding: 'utf8', valueEncoding: 'utf8' })
await db.put('key', 'value')
console.log(await db.get('key')) // { seq: 1, key: 'key', value: 'value' }
await db.put('key', 'value', { cas })
console.log(await db.get('key')) // { seq: 1, key: 'key', value: 'value' }
await db.put('key', 'value*', { cas })
console.log(await db.get('key')) // { seq: 2, key: 'key', value: 'value*' }
```

#### `{ seq, key, value } = await db.get(key)`

Get a key, value. If the key does not exist, `null` is returned.
`seq` is the hypercore version at which this key was inserted.

#### `await db.del(key, [options])`

Delete a key

Options include:

```js
{
  cas (prev, next) { return true }
}
```

##### Compare And Swap (cas)
You can pass a `cas` function as an option to `del` that controls whether the `del` succeeds. Given `bee.del(key, { cas })`, `cas` is passed the current node (i.e. `{ seq, key, value }`) in `bee` at `key`, and `del` succeeds only if `cas` returns `true` and fails otherwise.

```js
const cas = (prev) => prev.value === 'value*'
const db = new Hyperbee(core, { keyEncoding: 'utf8', valueEncoding: 'utf8' })
await db.put('key', 'value')
console.log(await db.get('key')) // { seq: 1, key: 'key', value: 'value' }
await db.del('key', { cas })
console.log(await db.get('key')) // { seq: 1, key: 'key', value: 'value' }
await db.put('key', 'value*')
console.log(await db.get('key')) // { seq: 2, key: 'key', value: 'value*' }
await db.del('key', { cas })
console.log(await db.get('key')) // null
```

#### `batch = db.batch()`

Make a new batch.

A batch is atomic: it is either processed fully or not at all.

#### `await batch.put(key, [value], [options])`

Insert a key into a batch.

Options include:

```js
{
  cas (prev, next) { return true } // see await db.put(key, value, { cas })
}
```

#### `{ seq, key, value } = await batch.get(key)`

Get a key, value out of a batch.

#### `await batch.del(key, [options])`

Delete a key into the batch.

Options include:

```js
{
  cas (prev, next) { return true } // see await db.del(key, { cas })
}
```

#### `await batch.flush()`

Commit the batch to the database.

#### `batch.destroy()`

Destroy a batch and releases any locks it has aquired on the db.
Call this if you want to abort a batch without flushing it.

#### `stream = db.createReadStream([options])`

Make a read stream. All entries in the stream are similar to the ones returned from .get and the
sort order is based on the binary value of the keys.

Options include:

``` js
{
  gt: 'only return keys > than this',
  gte: 'only return keys >= than this',
  lt: 'only return keys < than this',
  lte: 'only return keys <= than this',
  reverse: false // set to true to get them in reverse order,
  limit: -1 // set to the max number of entries you want
}
```

#### `const { seq, key, value } = await db.peek([options])`

Similar to doing a read stream and returning the first value, but a bit faster than that.

#### `stream = db.createHistoryStream([options])`

Create a stream of all entries ever inserted or deleted from the db.
Each entry has an additional `type` property indicating if it was a `put` or `del` operation.

Options include:

``` js
{
  live: false, // if true the stream will wait for new data and never end
  reverse: false, // if true get from the newest to the oldest
  gte: seq, // start with this seq (inclusive)
  gt: seq, // start after this index
  lte: seq, // stop after this index
  lt: seq, // stop before this index
  limit: -1 // set to the max number of entries you want
}
````

If any of the gte, gt, lte, lt arguments are `< 0` then
they'll implicitly be added with the version before starting so
doing `{ gte: -1 }` makes a stream starting at the last index.

#### `stream = db.createDiffStream(otherVersion, [options])`

Efficiently create a stream of the shallow changes between two versions of the db.
Each entry is sorted by key and looks like this:

``` js
{
  left: <the entry in the db>,
  right: <the entry in the other version>
}
```

If an entry exists in db but not in the other version, then `left` is set
and `right` will be null, and vice versa.

If the entries are causally equal (i.e. the have the same seq), they are not
returned, only the diff.

Currently accepts the same options as the read stream except for reverse.

#### `watcher = db.watch([range])`

Listens to changes that are on the optional `range`.

`range` options are the same as `createReadStream` except for reverse.

Usage example:
```js
for await (const [current, previous] of watcher) {
  console.log(current.version)
  console.log(previous.version)
}
```

Returns a new value after a change, `current` and `previous` are snapshots that are auto-closed before next value.

Don't close those snapshots yourself because they're used internally, let them be auto-closed.

`await watcher.ready()`

Waits until the watcher is loaded and detecting changes.

`await watcher.destroy()`

Stops the watcher. You could also stop it by using `break` in the loop.

#### `dbCheckout = db.checkout(version)`

Get a readonly db checkout of a previous version.

#### `dbCheckout = db.snapshot()`

Shorthand for getting a checkout for the current version.

#### `const sub = db.sub('sub-prefix', opts = {})`

Create a sub-database where all entries will be prefixed by a given value.

This makes it easy to create namespaces within a single Hyperbee.

Options include:
```js
{
  sep: Buffer.alloc(1), // A namespace separator
  valueEncoding, // optional sub valueEncoding (defaults to the parents)
  keyEncoding // optional sub keyEncoding (defaults to the parents)
}
```

For example:
```js
const rootDb = new Hyperbee(core)
const subDb = rootDb.sub('a')

// In rootDb, this will have the key ('a' + separator + 'b')
await subDb.put('b', 'hello')

// Returns { key: 'b', value: 'hello')
await subDb.get('b')
```

#### `db.version`

Current version.

#### `await db.ready()`

Makes sure internal state is loaded. Call this once before checking the version if you haven't called any of the other APIs.

#### `await Hyperbee.isHyperbee(core, opts?)`

Returns true if the core contains a hyperbee, false otherwise.

Throws if the first block could not be loaded.
This can only happen when the likes of `wait: false` or `timeout: someTimeout` are set
(see the documentation for `hypercore.get`).
The default behaviour is to wait until the first block is available
(thereafter returning either true or false).

