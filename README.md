# Hyperbee 🐝

An append-only Btree running on a Hypercore.
Allows sorted iteration and more.

```
npm install hyperbee
```

## Usage

``` js
const Hyperbee = require('hyperbee')
const db = new Hyperbee(feed, {
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

Some of the internals are still being tweaked but overall the API and feature set is pretty
stable if you want to try it out.

All of the above methods work with sparse feeds, meaning only a small subset of the full
feed is downloaded to satisfy you queries.

## API

#### `const db = new Hyperbee(feed, [options])`

Make a new Hyperbee instance. `feed` should be a [Hypercore](https://github.com/hypercore-protocol/hypercore).

Options include:

```
{
  keyEncoding: 'utf-8' | 'binary' | 'ascii', // or some abstract encoding
  valueEncoding: <same as above>
}
```

Note that currently read/diff streams sort based on the *encoded* value of the keys.

#### `await db.put(key, [value])`

Insert a new key. Value can be optional. If you are inserting a series of data atomically,
or you just have a batch of inserts/deletions available using a batch can be much faster
than simply using a series of puts/dels on the db.

#### `{ seq, key, value } = await db.get(key)`

Get a key, value. If the key does not exist, `null` is returned.
`seq` is the hypercore version at which this key was inserted.

#### `await db.del(key)`

Delete a key

#### `batch = db.batch()`

Make a new batch.

#### `await batch.put(key, [value])`

Insert a key into a batch.

#### `{ seq, key, value } = await batch.get(key)`

Get a key, value out of a batch.

#### `await batch.del(key)`

Delete a key into the batch.

#### `await batch.flush()`

Commit the batch to the database.

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

Options include:

``` js
{
  reverse: false, // if true get from the newest to the oldest
  since: seq // start with this seq,
  limit: -1 // set to the max number of entries you want
}
````

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

#### `dbCheckout = db.checkout(version)`

Get a readonly db checkout of a previous version.

#### `dbCheckout = db.snapshot()`

Shorthand for getting a checkout for the current version.

#### `db.version`

Current version.

#### `await db.ready()`

Makes sure internal state is loaded. Call this once before checking the version if you haven't called any of the other APIs.
