const Hyperb = require('../../')
const hypercore = require('hypercore')
const split2 = require('split2')
const fs = require('fs')

const db = new Hyperb(hypercore('./db'))

main()

async function main () {
  const s = fs.createReadStream('title.basics.tsv').pipe(split2())

  setInterval(function () {
    console.log(n)
  }, 1000).unref()

  let n = 0
  let first = true
  let batch = db.batch()
  for await (const line of s) {
    if (first) {
      first = false
      continue
    }

    const [id, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres] = line.split('\t')

    const data = {
      id,
      titleType,
      primaryTitle,
      isAdult: isAdult !== '0',
      startYear: startYear === '\\N' ? 0 : Number(startYear),
      endYear: endYear === '\\N' ? 0 : Number(endYear),
      runtimeMinutes: Number(runtimeMinutes) || 0,
      genres: genres === '\\N' ? [] : genres.split(',')
    }

    n++
    await batch.put('ids!' + data.id, JSON.stringify(data))

    if (batch.length > 4096) {
      await batch.flush()
      batch = db.batch()
    }
  }

  if (batch.length) await batch.flush()
}
