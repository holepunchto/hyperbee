// Type declarations for the holepunchto/hyperbee public API.
/// <reference types="node" />
// NOTE: could not resolve external type(s) CASComparator, Readable, Stream — rendered as `any`; add a manual import/type if one is available.

import type Hypercore from 'hypercore'

/**
 * A single entry as returned by reads such as `db.get()`, `db.peek()`, and the
read stream. `key` and `value` are decoded with the bee's `keyEncoding` /
`valueEncoding` (raw `Buffer`s when no encoding is set).
 */
export interface Entry {
  /** Hypercore index (block number) at which this entry was inserted. */
  seq: number
  /** The entry's key, decoded per `keyEncoding`. */
  key: any
  /** The entry's value, decoded per `valueEncoding` (`null` for a deletion). */
  value: any
}

/**
 * Options for `new Hyperbee()`.
 */
export interface HyperbeeOptions {
  /** Key encoding: `"binary"` (default), `"utf-8"`, `"ascii"`, `"json"`, or an abstract-encoding instance. */
  keyEncoding?: string | object
  /** Value encoding; same options as `keyEncoding`. */
  valueEncoding?: string | object
}

/**
 * Options for `db.put()`, `db.del()` and their batch equivalents.
 */
export interface PutOptions {
  /** Compare-and-swap comparator; the write only proceeds when it returns `true`. */
  cas?: any
}

/**
 * Range bounds for read, diff, and watch streams. Keys are compared by their
encoded byte value.
 */
export interface Range {
  /** Only return keys greater than this. */
  gt?: any
  /** Only return keys greater than or equal to this. */
  gte?: any
  /** Only return keys less than this. */
  lt?: any
  /** Only return keys less than or equal to this. */
  lte?: any
}

/**
 * Options for `db.createReadStream()` and `db.peek()`.
 */
export interface ReadStreamOptions {
  /** Yield entries in reverse (descending) order. */
  reverse?: boolean
  /** Maximum number of entries to return (`-1` for no limit). */
  limit?: number
}

/**
 * Options for `db.createHistoryStream()`.
 */
export interface HistoryStreamOptions {
  /** Keep the stream open and wait for new data instead of ending. */
  live?: boolean
  /** Iterate from newest to oldest. */
  reverse?: boolean
  /** Start at this seq (inclusive); negative values are added to the current version. */
  gte?: number
  /** Start after this seq. */
  gt?: number
  /** Stop at this seq (inclusive). */
  lte?: number
  /** Stop before this seq. */
  lt?: number
  /** Maximum number of entries to return (`-1` for no limit). */
  limit?: number
}

/**
 * Options for `db.sub()`.
 */
export interface SubOptions {
  /** Namespace separator placed between the prefix and key (defaults to the parent's separator). */
  sep?: Buffer
  /** Key encoding for the sub (defaults to the parent's). */
  keyEncoding?: string | object
  /** Value encoding for the sub (defaults to the parent's). */
  valueEncoding?: string | object
}

/**
 * Options for point reads such as `db.get()` and `db.getBySeq()`. The encoding
overrides apply to this call only; remaining options are forwarded to the
underlying `core.get()`.
 */
export interface GetOptions {
  /** Override the key encoding for this read. */
  keyEncoding?: string | object
  /** Override the value encoding for this read. */
  valueEncoding?: string | object
  /** Wait for the block to download from a peer if it isn't local. */
  wait?: boolean
  /** Max milliseconds to wait for a block (`0` = no timeout). */
  timeout?: number
}

/**
 * Options for `db.batch()`.
 */
export interface BatchOptions {
  /** Override the key encoding for this batch. */
  keyEncoding?: string | object
  /** Override the value encoding for this batch. */
  valueEncoding?: string | object
  /** Update the underlying core before the first operation resolves. */
  update?: boolean
}

/**
 * Options for `db.watch()`.
 */
export interface WatchOptions {
  /** Key encoding for the yielded snapshots (defaults to the bee's). */
  keyEncoding?: string | object
  /** Value encoding for the yielded snapshots (defaults to the bee's). */
  valueEncoding?: string | object
}

/**
 * Options for `db.getAndWatch()`.
 */
export interface EntryWatchOptions {
  /** Key encoding for the watched entry (defaults to the bee's). */
  keyEncoding?: string | object
  /** Value encoding for the watched entry (defaults to the bee's). */
  valueEncoding?: string | object
}

/**
 * Options for `db.checkout()` and `db.snapshot()`.
 */
export interface CheckoutOptions {
  /** Key encoding for the snapshot (defaults to the bee's). */
  keyEncoding?: string | object
  /** Value encoding for the snapshot (defaults to the bee's). */
  valueEncoding?: string | object
}

export class Hyperbee {
  /**
   * Make a new Hyperbee instance. `core` should be a [Hypercore](https://github.com/holepunchto/hypercore).
   * @param core - `core` should be a [Hypercore](https://github.com/holepunchto/hypercore).
   */
  constructor(core: Hypercore, opts?: HyperbeeOptions)

  /**
   * Number that indicates how many modifications were made, useful as a version identifier.
   */
  readonly version: number

  /**
   * String containing the id (z-base-32 of the public key) identifying this bee.
   */
  readonly id: string

  /**
   * Buffer containing the public key identifying this bee.
   */
  readonly key: Buffer

  /**
   * Buffer containing a key derived from `db.key`.
   */
  readonly discoveryKey: Buffer

  /**
   * Boolean indicating if we can put or delete data in this bee.
   */
  readonly writable: boolean

  /**
   * Boolean indicating if we can read from this bee. After closing the bee this will be `false`.
   */
  readonly readable: boolean

  /**
   * See more about how replicate works at core.replicate.
   * @param isInitiator - `true`/`false` to open a fresh replication stream, or an existing stream to replicate over.
   * @param opts - Replication options, forwarded to `core.replicate`.
   * @returns The replication stream.
   */
  replicate(isInitiator: boolean | any, opts: object): any

  update(opts: any): any

  /**
   * Similar to doing a read stream and returning the first value, but a bit faster than that.
   * @param range - Range bounds to search within.
   * @param opts - Read options (`reverse` selects the last entry instead of the first).
   * @returns The first matching entry, or `null` if the range is empty.
   */
  peek(range?: Range, opts?: ReadStreamOptions): Promise<Entry | null>

  createRangeIterator(range: any, opts?: any): any

  /**
   * Make a read stream. Sort order is based on the binary value of the keys.
   * @param range - `range` should specify the range you want to read and looks like this:
   * @returns A stream of `Entry` objects.
   */
  createReadStream(range?: Range, opts?: ReadStreamOptions): any

  /**
   * Create a stream of all entries ever inserted or deleted from the db.
   * @returns A stream of `Entry` objects, each with an added `type` (`'put'` or `'del'`).
   */
  createHistoryStream(opts?: HistoryStreamOptions): any

  /**
   * Efficiently create a stream of the shallow changes between two versions of the db.
   * @param right - The other version to diff against: a version number, or another bee/snapshot.
   * @param range - `options` are the same as `db.createReadStream`, except for `reverse`.
   * @param opts - Read options.
   * @returns A stream of `{ left, right }` pairs, where each side is an `Entry` or `null`.
   */
  createDiffStream(right: number | Hyperbee, range?: Range, opts?: ReadStreamOptions): any

  /**
   * Get a key's value. Returns `null` if key doesn't exists.
   * @param key - The key to look up (encoded per `keyEncoding`).
   * @param opts - Read options.
   * @returns The entry, or `null` if the key is not present.
   */
  get(key: any, opts: GetOptions): Promise<Entry | null>

  /**
   * Get the key and value from a block number.
   * @param seq - `seq` is the Hypercore index.
   * @param opts - Read options.
   * @returns The block's key and value.
   */
  getBySeq(seq: number, opts?: GetOptions): Promise<{ key: any; value: any }>

  /**
   * Insert a new key. Value can be optional.
   * @param key - The key to insert (encoded per `keyEncoding`).
   * @param value - The value to store; optional for key-only entries.
   * @returns Resolves once the write is committed to the core.
   */
  put(key: any, value?: any, opts?: PutOptions): Promise<void>

  /**
   * Make a new atomic batch that is either fully processed or not processed at all.
   * @param opts - Batch options.
   * @returns A new batch; queue writes on it and call `flush()` to commit.
   */
  batch(opts: BatchOptions): Batch

  /**
   * Delete a key.
   * @param key - The key to delete (encoded per `keyEncoding`).
   * @returns Resolves once the deletion is committed to the core.
   */
  del(key: any, opts?: PutOptions): Promise<void>

  /**
   * Listens to changes that are on the optional `range`.
   * @param range - Range bounds to narrow what changes are watched; same as `db.createReadStream` except `reverse`.
   * @param opts - Watch options.
   * @returns Returns a new value after a change, `current` and `previous` are snapshots that are auto-closed before next value.
   */
  watch(range?: Range, opts?: WatchOptions): Watcher

  /**
   * `entryWatcher.node` contains the current entry in the same format as the result of `bee.get(key)`, and will be updated as it changes.
   * @param key - The key to watch (encoded per `keyEncoding`).
   * @param opts - Watch options.
   * @returns Returns a watcher which listens to changes on the given key.
   */
  getAndWatch(key: any, opts?: EntryWatchOptions): Promise<EntryWatcher>

  clearUnlinked(options?: any): Promise<any>

  /**
   * Get a read-only snapshot of a previous version.
   * @param version - The version (see `db.version`) to check out.
   * @param opts - Snapshot options.
   * @returns A read-only bee pinned to that version.
   */
  checkout(version: number, opts?: CheckoutOptions): Hyperbee

  /**
   * Shorthand for getting a checkout for the current version.
   * @param opts - Snapshot options.
   * @returns A read-only bee pinned to the current version.
   */
  snapshot(opts: CheckoutOptions): Hyperbee

  /**
   * Create a sub-database where all entries will be prefixed by a given value.
   * @param prefix - The namespace prefix applied to every key.
   * @returns A bee scoped to the prefixed keyspace.
   */
  sub(prefix: string | Buffer, opts?: SubOptions): Hyperbee

  /**
   * `options` are the same as the `core.get` method.
   * @param opts - `options` are the same as the `core.get` method.
   * @returns Returns the header contained in the first block.
   */
  getHeader(opts?: GetOptions): Promise<object>

  /**
   * This requests the first block on the core, so it can throw depending on the options.
   * @param core - The core to inspect.
   * @param opts - `options` are the same as the `core.get` method.
   * @returns Returns `true` if the core contains a Hyperbee, `false` otherwise.
   */
  static isHyperbee(core: Hypercore, opts?: GetOptions): Promise<boolean>

  /**
   * Waits until internal state is loaded.
   */
  ready(): Promise<any>

  /**
   * Fully close this bee, including its core.
   */
  close(): Promise<any>

  readonly opened: any

  readonly closed: any

  on(event: any, listener: any): any

  once(event: any, listener: any): any

  off(event: any, listener: any): any

  emit(event: any, arg1?: any): any

  feed: any

  /**
   * The underlying Hypercore backing this bee.
   */
  core: Hypercore

  keyEncoding: any

  valueEncoding: any

  extension: any

  metadata: any

  lock: any

  sep: any

  readonly: any

  prefix: any

  alwaysDuplicate: any
}

declare class Batch {
  constructor(tree: any, core: any, batchLock: any, cache: any, options?: any)

  ready(): Promise<any>

  lock(): Promise<any>

  readonly version: any

  getRoot(ensureHeader: any): Promise<any>

  getKey(seq: any): Promise<any>

  getBlock(seq: any): Promise<any>

  peek(range: any, opts: any): any

  createRangeIterator(range: any, opts?: any): any

  createReadStream(range: any, opts: any): any

  getBySeq(seq: any, opts: any): Promise<any>

  /**
   * Get a key, value out of a batch.
   * @param key - The key to look up (encoded per the batch's `keyEncoding`).
   * @param opts - Read options.
   * @returns The pending or committed entry visible in the batch, or `null` if missing.
   */
  get(key: any, opts: GetOptions): Promise<Entry | null>

  links(key: any, seq: any): Promise<any>

  /**
   * Insert a key into a batch.
   * @param key - The key to insert (encoded per the batch's `keyEncoding`).
   * @param value - The value to store (encoded per the batch's `valueEncoding`); optional.
   * @param opts - `options` are the same as `db.put` method.
   * @returns Resolves once the write is queued in the batch.
   */
  put(key: any, value?: any, opts?: PutOptions): Promise<void>

  /**
   * Delete a key into the batch.
   * @param key - The key to delete (encoded per the batch's `keyEncoding`).
   * @param opts - `options` are the same as `db.del` method.
   * @returns Resolves once the deletion is queued in the batch.
   */
  del(key: any, opts?: PutOptions): Promise<void>

  /**
   * Destroy a batch, and releases any locks it has acquired on the db.
   * @returns Resolves once the batch is discarded and its lock released.
   */
  close(): Promise<void>

  destroy(): any

  toBlocks(): any

  /**
   * Commit the batch to the database, and releases any locks it has acquired.
   * @returns Resolves once the queued operations are committed and the lock released.
   */
  flush(): Promise<void>

  tree: any

  feed: any

  core: any

  index: any

  blocks: any

  autoFlush: any

  maxBlocksCached: any

  rootSeq: any

  root: any

  length: any

  checkout: any

  options: any

  locked: any

  batchLock: any

  onseq: any

  appending: any

  isSnapshot: any

  shouldUpdate: any

  updating: any

  encoding: any
}

declare class Watcher {
  constructor(bee: any, range: any, opts?: any)

  next(): Promise<any>

  return(): Promise<any>

  destroy(): any

  ready(): Promise<any>

  close(): Promise<any>

  readonly opened: any

  readonly closed: any

  emit(event: any, arg1?: any): any

  keyEncoding: any

  valueEncoding: any

  index: any

  bee: any

  core: any

  latestDiff: any

  range: any

  map: any

  current: any

  previous: any

  currentMapped: any

  previousMapped: any

  stream: any

  on(event: 'update', listener: () => void): this
}

declare class EntryWatcher {
  constructor(bee: any, key: any, opts?: any)

  ready(): Promise<any>

  close(): Promise<any>

  readonly opened: any

  readonly closed: any

  emit(event: any, arg1?: any): any

  keyEncoding: any

  valueEncoding: any

  index: any

  bee: any

  key: any

  node: any

  on(event: 'error', listener: (e: any) => void): this
  on(event: 'update', listener: () => void): this
}

export default Hyperbee
