import { getGzippedS3Stream } from './s3-stream.js'
import fs from 'fs'
import minimist from 'minimist'
import XMLStream from 'node-xml-stream'
import { EventEmitter } from 'events'
import { pMapIterable } from 'p-map'

const args = minimist(process.argv.slice(2), {
  default: { records: 100, parallel: 1, output: 'discogs-tpb.tsv' },
  alias: { r: 'records', p: 'parallel', o: 'output' }
})

const COLUMNS = [
  'id',
  'main_release',
  'name',
  'title',
  'year',
  'genre',
  'style',
  'data_quality',
  'hashes',
  'seeders'
]

// Create an async iterator for XML events
function createXmlEventEmitter (stream) {
  const emitter = new EventEmitter()
  const parser = new XMLStream()
  stream.pipe(parser)

  parser.on('opentag', (name, attrs) => {
    emitter.emit('event', { type: 'opentag', name, attrs })
  })
  parser.on('text', (text) => {
    emitter.emit('event', { type: 'text', text })
  })
  parser.on('closetag', (name) => {
    emitter.emit('event', { type: 'closetag', name })
  })
  parser.on('error', (error) => {
    emitter.emit('error', error)
  })
  parser.on('end', () => {
    emitter.emit('end')
  })

  return emitter
}

// Create an async iterator for master records
async function * masterIterator (stream) {
  const emitter = createXmlEventEmitter(stream)
  let currentMaster = null
  let currentPath = []

  try {
    for await (const event of emitter) {
      switch (event.type) {
        case 'opentag':
          if (event.name === 'master') {
            currentMaster = { id: event.attrs.id }
            currentPath = ['master']
          } else if (currentMaster) {
            currentPath.push(event.name)
          }
          break
        case 'text':
          if (currentMaster && currentPath.length > 1) {
            const field = currentPath[currentPath.length - 1]
            currentMaster[field] = event.text
          }
          break
        case 'closetag':
          if (event.name === 'master') {
            yield currentMaster
            currentMaster = null
            currentPath = []
          } else if (currentPath.length > 0) {
            currentPath.pop()
          }
          break
      }
    }
  } finally {
    stream.destroy()
  }
}

const lookupDiscogsMaster = async (master) => {
  try {
    const query = `${master.name} ${master.title}`
    const url = `https://apibay.org/q.php?q=${encodeURIComponent(query)}&cat=100`
    const result = await fetch(url)
    if (!result.ok) {
      throw new Error(`HTTP error! status: ${result.status}`)
    }
    const text = await result.text()
    if (!text) {
      throw new Error('Empty response from TPB API')
    }
    const items = JSON.parse(text)

    // Check if we got a "no results" response
    if (items.length === 1 && items[0].id === '0') {
      console.log(`#${master.id}: ${master.name} - ${master.title} | No results`)
      return { master, tpbResults: [] }
    }

    // Remove duplicates based on info_hash
    const uniqueResults = items.filter((result, index, self) =>
      index === self.findIndex(r => r.info_hash === result.info_hash)
    )

    // Sort by seeders (most seeded first)
    uniqueResults.sort((a, b) => b.seeders - a.seeders)

    console.log(`#${master.id}: ${master.name} - ${master.title} | ${uniqueResults.length} results`)

    return {
      master,
      tpbResults: uniqueResults
    }
  } catch (e) {
    console.error(`Error while looking up discogs master #${master.id}:`, e)
    return { master, tpbResults: [] }
  }
}

const writeResult = (stream, master, tpbResults) => {
  if (tpbResults.length === 0) return

  const row = COLUMNS.map(field => {
    if (field === 'hashes') {
      return tpbResults.map(r => r.info_hash).join(',')
    } else if (field === 'seeders') {
      return tpbResults.map(r => r.seeders).join(',')
    } else {
      return master[field] ? master[field].toString().replace(/\t/g, ' ') : ''
    }
  }).join('\t')

  stream.write(row + '\n')
}

async function main () {
  try {
    console.log(`Starting to stream Discogs XML (processing ${args.records} records with max ${args.parallel} concurrent requests)...`)
    const stream = await getGzippedS3Stream(
      'discogs-data-dumps',
      'data/2025/discogs_20250401_masters.xml.gz'
    )

    let count = 0

    // Delete existing file if it exists
    if (fs.existsSync(args.output)) {
      fs.unlinkSync(args.output)
    }

    // Create write stream and write header
    const outputStream = fs.createWriteStream(args.output)
    outputStream.write(COLUMNS.join('\t') + '\n')

    // Process a single master record
    const processMaster = async (master) => {
      const result = await lookupDiscogsMaster(master)
      writeResult(outputStream, result.master, result.tpbResults)
    }

    // Create an async iterator that yields masters
    async function * masters () {
      for await (const master of masterIterator(stream)) {
        if (count >= args.records) break
        count++
        console.log(`Found master #${master.id}: ${master.name} - ${master.title}`)
        yield master
      }
    }

    // Process masters in parallel as they arrive
    await pMapIterable(
      masters(),
      master =>
        processMaster(master).catch(error => {
          console.error(`Error processing master #${master.id}:`, error)
        }),
      { concurrency: args.parallel }
    )

    outputStream.end()
    console.log(`Finished processing and writing results to ${args.output}`)
  } catch (error) {
    console.error('Error:', error)
  }
}

// Only run if this is the main module
if (process.argv[1] === new URL(import.meta.url).pathname) {
  main()
}