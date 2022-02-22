const Hyperbee = require('../../')
const Hypercore = require('hypercore')

const core = new Hypercore('./db')
const db = new Hyperbee(core)

require('@hyperswarm/replicator')(db.feed, {
  announce: true,
  lookup: true,
  live: true
})

db.feed.ready(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
})

module.exports = db
