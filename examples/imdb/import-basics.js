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
  let max = 4096
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
      originalTitle,
      isAdult: isAdult !== '0',
      startYear: startYear === '\\N' ? 0 : Number(startYear),
      endYear: endYear === '\\N' ? 0 : Number(endYear),
      runtimeMinutes: Number(runtimeMinutes) || 0,
      genres: genres === '\\N' ? [] : genres.split(',')
    }

    const key = 'ids!' + data.id

    n++
    const prev = await batch.get(key)
    const d = JSON.stringify(data)

    if (!prev || !prev.value || prev.value.toString() !== d) {
      await batch.put(key, d)
    } else {
      max--
    }

    if (batch.length > max) {
      max = 4096
      await batch.flush()
      batch = db.batch()
    }
  }

  if (batch.length) await batch.flush()
}
