import { createHash } from 'node:crypto'
import pMap from 'p-map'
import _ from 'lodash'
import fs from 'node:fs'
import { strToDate } from './util.js'

const fetchBookHash = async (index) => {
  const url = `https://www.gutenberg.org/cache/epub/${index}/pg${index}-h.zip`
  const response = await fetch(url)
  if (response.status !== 200) {
    throw new Error(`status: ${response.status}`)
  }
  const stream = response.body
  const hash = createHash('sha256')
  for await (const chunk of stream) {
    hash.update(chunk)
  }
  const digest = hash.digest('hex')
  return [url, digest]
}

const fetchBookHashes = async (n) => {
  const indices = _.range(1, n + 1)
  const results = await pMap(indices, async (i) => {
    try {
      const [url, digest] = await fetchBookHash(i)
      console.log(url, digest)
      return [url, digest]
    } catch (e) {
      console.log(e)
      return undefined
    }
  }, { concurrency: 32 })
  return Object.fromEntries(results.filter(x => x))
}

const main = async (n) => {
  const results = await fetchBookHashes(n)
  const date = (new Date()).toISOString()
  const data = {
    date,
    files: results,
    digest: 'SHA-256',
    service: 'timestamper',
    source: 'https://github.com/arthuredelstein/timestamper'
  }
  fs.writeFileSync('gutenberg.json', JSON.stringify(data))
}
