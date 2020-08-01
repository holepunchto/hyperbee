const Hyperb = require('../../')
const hypercore = require('hypercore')

const db = new Hyperb(hypercore('./db-clone', '95c4bff66d3faa78cf8c70bd070089e5e25b4c9bcbbf6ce5eb98e47b3129ca93', { sparse: true }))

const swarm = require('@hyperswarm/replicator')(db.feed, {
  announce: true,
  lookup: true,
  live: true
})

db.feed.timeouts.update = function (cb) {
  swarm.flush(function () {
    db.feed.timeouts.update = (cb) => cb()
    cb()
  })
}

db.feed.ready(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
})

db.get('ids!' + process.argv[2]).then(function (node) {
  console.log(node && JSON.parse(node.value.toString()))
})
