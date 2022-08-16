const Hyperbee = require('../../')
const Hypercore = require('hypercore')
const Hyperswarm = require('hyperswarm')

const db = new Hyperbee(new Hypercore('./db-clone', '95c4bff66d3faa78cf8c70bd070089e5e25b4c9bcbbf6ce5eb98e47b3129ca93'))
const swarm = new Hyperswarm()

swarm.on('connection', c => db.feed.replicate(c))

db.feed.ready(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))

  const done = db.feed.findingPeers()

  swarm.join(db.feed.discoveryKey)
  swarm.flush().then(done, done)
})

db.get('ids!' + process.argv[2]).then(function (node) {
  console.log(node && JSON.parse(node.value.toString()))
})
