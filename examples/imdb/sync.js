const Hyperbee = require('../../')
const Hypercore = require('hypercore')
const Hyperswarm = require('hyperswarm')

const db = new Hyperbee(new Hypercore('./db'))
const swarm = new Hyperswarm()

swarm.on('connection', c => db.feed.replicate(c))

db.feed.ready().then(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
  swarm.join(db.feed.discoveryKey)
})

module.exports = db
