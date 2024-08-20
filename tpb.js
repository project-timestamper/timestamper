const zlib = require('node:zlib')
const { pipeline } = require('node:stream')
const { promisify } = require('node:util')
const streamPipeline = promisify(pipeline)
const readline = require('node:readline')
const { zipObject } = require('lodash')
const fs = require('node:fs')
const { stampHashes, stampAndCollectHashes } = require('./timestamp.js')

const imdbList = async function * () {
  const response = await fetch('https://datasets.imdbws.com/title.basics.tsv.gz')
  const gunzip = zlib.createGunzip()
  gunzip.on('error', (err) => console.log(err))
  streamPipeline(response.body, gunzip)
  const rl = readline.createInterface({
    input: gunzip,
    crlfDelay: Infinity
  })
  for await (const line of rl) {
    // Each line in input.txt will be successively available here as `line`.
    yield line
  }
}

const splitLines = async function * (lines) {
  for await (const line of lines) {
    yield line.split('\t')
  }
}

const makeRowObjects = async function * (rows) {
  let headings
  for await (const items of rows) {
    if (headings === undefined) {
      headings = items
    } else {
      yield zipObject(headings, items)
    }
  }
}

const collectTitleTypes = async function (objects) {
  const typeSet = new Set()
  for await (const obj of objects) {
    typeSet.add(obj.titleType)
  }
  return [...typeSet]
}

const filterTitleType = async function * (objects) {
  for await (const obj of objects) {
    if (['movie', 'tvMiniSeries', 'tvMovie', 'tvSeries', 'tvSpecial'].includes(obj.titleType)) {
      yield obj.tconst
    }
  }
}

const asList = async function (items) {
  const result = []
  for await (const item of items) {
    result.push(item)
  }
  return result
}

const count = async function (items) {
  let count = 0
  for await (const item of items) {
    ++count
  }
  return count
}

let titleCount = 0

const lookupImdbTitle = async (imdbID) => {
  try {
    console.log(++titleCount, imdbID)
    const result = await fetch(`https://apibay.org/q.php?q=${imdbID}`)
    return JSON.parse(await result.text())
  } catch (e) {
    console.log('Error while looking up imdb title:', e)
    return null
  }
}

const lookupAll = async (imdbIDs) => {
  const { default: pMap } = await import('p-map')
  const raw = await pMap(imdbIDs, lookupImdbTitle,
    { concurrency: 32, stopOnError: false })
  return raw.filter(item => item && !(item.length === 1 && item[0].id === '0'))
}

const writeResults = (results) => {
  const stream = fs.createWriteStream('tpb-movies.txt', { flags: 'a' })
  for (const shortList of results) {
    for (const item of shortList) {
      const name = item.name.replaceAll(/\s/g, ' ').trim()
      stream.write(`${name}\t${item.imdb}\t${item.info_hash}\n`)
    }
  }
  stream.end()
}

const readResults = () => {
  const content = fs.readFileSync('./out/tpb-movies.txt').toString()
  const lines = content.split('\n')
  return lines.filter(line => line.length > 0).map(line => line.split('\t'))
}

const getResultHashes = () => {
  const results = readResults()
  return results.map(item => item[2])
}

const writeRows = (file, rows) => {
  fs.rmSync(file)
  const stream = fs.createWriteStream(file, { flags: 'a' })
  for (const row of rows) {
    stream.write(`${row}\n`)
  }
  stream.end()
}

const main = async () => {
  // const imdbItems = await asList(filterTitleType(makeRowObjects(splitLines(imdbList()))))
  // writeResults(await lookupAll(imdbItems))
  const hashes = getResultHashes()
  console.log('hashes.length: ', hashes.length)
  const hashPairs = Object.entries(await stampAndCollectHashes(hashes))
  console.log('hashPairs.length: ', hashPairs.length)
  writeRows('tpb-hash-collection.txt', hashPairs.map(([a, b]) => `${a}\t${b}`))
}

if (require.main === module) {
  main()
}

// 'movie, short, tvEpisode, tvMiniSeries, tvMovie, tvPilot, tvSeries, tvShort, tvSpecial, video, videoGame'
