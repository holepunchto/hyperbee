const { Readable } = require('streamx')
const safetyCatch = require('safety-catch')

class WatchStream extends Readable {
  constructor (bee, opts) {
    // no need to buffer future watches, setting hwm to 1 fixes that
    super({ highWaterMark: 1, signal: opts.signal })

    this.keyEncoding = opts.keyEncoding || bee.keyEncoding
    this.valueEncoding = opts.valueEncoding || bee.valueEncoding
    this.bee = bee
    this.core = bee.core
    this.opened = null

    this.map = opts.map || null
    this.unmap = opts.unmap || null

    this._index = bee._watchers.push(this) - 1
    this._resolveOnChange = null
    this._resolveYield = null
    this._yielding = false
    this._latestChanges = 0
    this._changes = 0
    this._eager = !!opts.eager
    this._truncated = false

    // lifecycle
    this._setupPromise = null
    this._teardownPromise = null

    this.on('newListener', autoFlowOnUpdate)
  }

  // if consumed as an async iterator, use that to slow down further...
  async * [Symbol.asyncIterator] () {
    const ite = Readable.prototype[Symbol.asyncIterator].call(this)

    for await (const data of ite) {
      this._yielding = new Promise((resolve) => { this._resolveYield = resolve })
      yield data
      this._resolveYield()
      this._resolveYield = null
    }
  }

  // triggered by bee
  _ontruncate () {
    // TODO: change the truncated stuff to an ancestor count thing
    this._truncated = true
    this._onappend()
  }

  // triggered by bee
  _onappend () {
    // TODO: this is a light hack / fix for non-sparse session reporting .length's inside batches
    // the better solution is propably just to change non-sparse sessions to not report a fake length
    if (!this.core.isAutobase && (!this.core.core || this.core.core.tree.length !== this.core.length)) return

    const resolve = this._resolveOnChange
    this._resolveOnChange = null
    this._latestChanges++
    if (resolve) resolve()
  }

  async _setup () {
    // overwrite me
  }

  async _teardown () {
    // overwrite me
  }

  async _check () {
    // overwrite me
    return null
  }

  async _open (cb) {
    try {
      await this.opened
    } catch (err) {
      return cb(err)
    }
    cb(null)
  }

  async _read (cb) {
    let data = null

    try {
      while (!this.destroying && data === null) {
        await this._waitForChanges()
        if (this.destroying) break
        await this._yielding
        if (this.destroying) break
        this._changes = this._latestChanges
        const truncated = this._truncated
        this._truncated = false
        data = await this._check()
      }
    } catch (err) {
      return cb(err)
    }

    if (data !== null) this.push(data)
    cb(null)
  }

  _predestroy () {
    this._teardownPromise = this._teardown()
    this._teardownPromise.catch(safetyCatch)

    // trigger any pending io
    if (this._resolveYield) this._resolveYield()
    this._onappend()
  }

  async _destroy (cb) {
    const top = this.bee._watchers.pop()

    if (top !== this) {
      top._index = this._index
      this.bee._watchers[top._index] = top
    }

    if (!this._teardownPromise) this._teardownPromise = this._teardown()

    try {
      await this._teardownPromise
    } catch (err) {
      return cb(err)
    }

    cb(null)
  }

  async _waitForChanges () {
    if (this.destroying) return

    if (this._eager === false && this._changes === this._latestChanges) {
      await new Promise(resolve => {
        this._resolveOnChange = resolve
      })
    }

    this._eager = false
  }
}

class RangeWatchStream extends WatchStream {
  constructor (bee, range, opts = {}) {
    super(bee, opts)

    this.range = range
    this.current = null
    this.previous = null
    this.map = opts.map || null
    this.unmap = opts.unmap || null

    this._mapped = null
    this._stream = null
    this._differ = opts.differ || defaultDiffer
    this._checkout = opts.checkout || (opts.eager ? 1 : 0)

    this.opened = this._setup()
    this.opened.catch(safetyCatch)
  }

  async _setup () {
    if (!this.bee.opened) await this.bee.ready()

    const opts = {
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding
    }

    this.current = this._checkout > 0
      ? this.bee.checkout(this._checkout, opts)
      : this.bee.snapshot(opts)
  }

  async _teardown () {
    if (this._stream && !this._stream.destroying) {
      this._stream.destroy()
    }

    if (this.unmap) {
      await this._unmapMaybe()
    }

    await Promise.all([this._closeCurrent(), this._closePrevious()])
  }

  async _check () {
    await this._unmapMaybe()
    await this._closePrevious()
    if (this.destroying) return null

    this.previous = this.current.snapshot()

    await this._closeCurrent()
    if (this.destroying) return null

    this.current = this.bee.snapshot({
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding
    })

    if (!(await this._didRangeChange())) return null

    if (!this.map) {
      return [this.current, this.previous]
    }

    this._mapped = await this.map(this.current, this.previous)
    if (this.destroying) {
      await this._unmapMaybe()
      return null
    }

    return this._mapped
  }

  async _didRangeChange () {
    this._stream = this._differ(this.current, this.previous, this.range)

    try {
      for await (const data of this._stream) { // eslint-disable-line
        return true
      }
    } finally {
      this._stream = null
    }

    return false
  }

  async _unmapMaybe () {
    if (this._mapped && this.unmap) {
      const data = this._mapped
      this._mapped = null
      await this.unmap(data)
    }
  }

  async _closeCurrent () {
    const snap = this.current
    if (snap) await snap.close()
    if (snap === this.current) this.current = null
  }

  async _closePrevious () {
    const snap = this.previous
    if (snap) await snap.close()
    if (snap === this.previous) this.previous = null
  }
}

class KeyWatchStream extends WatchStream {
  constructor (bee, key, opts = {}) {
    super(bee, { eager: true, ...opts })

    this.key = key
    this.current = null
    this.previous = null
    this.node = null

    this.session = null
    this.map = opts.map || null

    this.opened = this._setup()
    this.opened.catch(safetyCatch)
  }

  async _setup () {
    this.session = this.bee.session()
  }

  async _teardown () {
    if (!this.session) return
    return this.session.close()
  }

  async _check (truncated) {
    let newNode = null
    try {
      newNode = await this.session.get(this.key, {
        keyEncoding: this.keyEncoding,
        valueEncoding: this.valueEncoding
      })
    } catch (err) {
      if (err.code === 'SNAPSHOT_NOT_AVAILABLE') {
        // There was a truncate event before the get resolved
        // So this handler will run again anyway
        return null
      }
      if (this.bee.closing) {
        this.destroy()
        return null
      }
      this.destroy(err)
      return null
    }

    if (truncated || newNode?.seq !== this.node?.seq) {
      this.node = newNode
      if (this.current) this.previous = this.current
      this.current = this.map && this.node ? await this.map(this.node, this.session) : this.node
      if (this.destroying) return null
      return [this.current, this.previous]
    }

    return null
  }
}

module.exports = { RangeWatchStream, KeyWatchStream }

function autoFlowOnUpdate (name) {
  if (name === 'update') {
    this.resume()
    this.on('data', () => this.emit('update'))
  }
}

function defaultDiffer (currentSnap, previousSnap, opts) {
  return currentSnap.createDiffStream(previousSnap.version, opts)
}
