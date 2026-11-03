function treeNodeId(node) {
  // Pending batch nodes keep offset 0 until flush and are unique objects.
  // Persisted getTreeNode() allocates a fresh wrapper each time, so identity
  // must be seq:offset (memoised on Child.value within a single edge).
  if (node.block.pendingIndex) return node
  return node.block.seq + ':' + node.offset
}

function assertNoTreeCycle(seen, node) {
  const id = treeNodeId(node)
  if (seen.has(id)) {
    const err = new Error('Invalid Hyperbee tree: cycle detected')
    err.code = 'INVALID_TREE'
    throw err
  }
  seen.add(id)
}

module.exports = { treeNodeId, assertNoTreeCycle }
