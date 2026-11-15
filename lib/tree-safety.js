const { INVALID_OPERATION } = require('hypercore-errors')

// B-tree height is O(log_t n). Anything this deep is a child-pointer cycle
// (e.g. a node whose child points at itself). Without a bound, Batch._get's
// while(true) spins on already-resolved awaits and starves the event loop
// so even `timeout` never fires (#206).
const MAX_TREE_DEPTH = 64

function assertTreeDepth(depth) {
  if (depth > MAX_TREE_DEPTH) {
    throw INVALID_OPERATION('Invalid hyperbee tree: cycle in child pointers')
  }
}

module.exports = { assertTreeDepth, MAX_TREE_DEPTH }
