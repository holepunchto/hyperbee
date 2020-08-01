const Hyperb = require('../../')
const hypercore = require('hypercore')

const db = new Hyperb(hypercore('./db', { sparse: true }))

require('@hyperswarm/replicator')(db.feed, {
  announce: true,
  lookup: true,
  live: true
})

db.feed.ready(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
})

module.exports = db
