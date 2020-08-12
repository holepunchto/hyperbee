const hypercore = require('hypercore')
const Hyperb = require('../../')
const db = new Hyperb(hypercore('./db-clone', '95c4bff66d3faa78cf8c70bd070089e5e25b4c9bcbbf6ce5eb98e47b3129ca93', { sparse: true }))

require('@hyperswarm/replicator')(db.feed, {
  announce: true,
  lookup: true,
  live: true
})

// let bytes = 0
// let blocks = 0

// db.feed.on('download', function (index, buf) {
//   console.log('block downloaded #' + index, 'total-bytes=' + (bytes += buf.length), 'total-blocks=' + (++blocks))
// })

db.feed.ready(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
})

db.feed.maxRequests = 512
db.feed.once('peer-open', async function () {
  console.log('Got a peer, ready for testing')

  const ids = [
    'tt0111161',
    'tt0068646',
    'tt0110912',
    'tt0073486',
    'tt1675434'
  ]

  const times = []
  for (const id of ids) {
    const start = Date.now()
    const node = await db.get('ids!' + id)
    times.push(Date.now() - start)
    console.log(node.value.toString())
  }

  console.log('times:', times)
  const avg = times.reduce((a, b) => a + b) / times.length
  console.log('AVERAGE TIME:', avg)
})
