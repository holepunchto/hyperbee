module.exports = class HyperbeeError extends Error {
  constructor (msg, code, fn = HyperbeeError) {
    super(`${code}: ${msg}`)
    this.code = code

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, fn)
    }
  }

  get name () {
    return 'HyperbeeError'
  }

  static BLOCK_NOT_AVAILABLE (msg = 'Block is not available') {
    return new HyperbeeError(msg, 'BLOCK_NOT_AVAILABLE', HyperbeeError.BLOCK_NOT_AVAILABLE)
  }
}
