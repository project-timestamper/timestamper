import fs from 'node:fs'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { stampHashes } from './timestamp'
import { findFirstInstance, readFilePart } from './util'
import path from 'node:path'
import { createHash } from 'node:crypto'
import OpenTimestamps from 'opentimestamps'

installIntoGlobal()

const getInsertLines = async function * (path, tableName) {
  let nextStart = 0
  while (true) {
    const { position } = await findFirstInstance(path, `INSERT INTO \`${tableName}\``, { start: nextStart })
    if (position < 0) {
      break
    }
    const { position: end, buf } = await findFirstInstance(path, ';\n', { start: position, accumulate: true })
    nextStart = end + 1
    yield buf
  }
}

const getData = async function * (path, tableName) {
  const lines = await getInsertLines(path, tableName)
  for await (const line of lines) {
    const lineString = line.toString()
    const [header, rows] = lineString.split(' VALUES ')
    const rows2 = rows.replaceAll('(', '[').replaceAll(')', ']').replaceAll('\'', '"').replaceAll(',NULL,', ',null,')
    let rows3
    try {
      rows3 = JSON.parse('[' + rows2 + ']')
    } catch (e) {
      console.log(rows2); throw e
    }
    const header2 = header.split('(')[1]
    const header3 = '[' + header2.replaceAll('`', '"').replaceAll(')', ']')
    let header4
    try {
      header4 = JSON.parse(header3)
    } catch (e) {
      console.log(header3); throw e
    }
    const final = rows3.map(row => {
      const result = {}
      for (let i = 0; i < header4.length; ++i) {
        result[header4[i]] = row[i]
      }
      return result
    })
    yield final
  }
}

const concat = async function * (arrays) {
  for await (const array of arrays) {
    for (const item of array) {
      yield item
    }
  }
}

const extractSha256 = async function (path, tableName) {
  const data1 = await getData(path, tableName)
  const data2 = await concat(data1)
  const data3 = data2.map(x => x.sha256)
  const fh = await fs.promises.open(path + '_sha256.txt', 'w')
  for await (const data of data3) {
    await fh.write(data + '\n')
  }
  await fh.close()
}

const readHashes = function (path) {
  const content = fs.readFileSync(path).toString()
  const lines = content.split('\n')
  return lines.filter(r => r.length === 64)
}

const saveAll = async function (hashes, path, stepSize) {
  for (let i = 0; i < hashes.length; i += stepSize) {
    await stampAndSaveHashes(path, hashes.slice(i, i + stepSize))
  }
}

const partitionByPrefix = (hashes, prefixLength) => {
  const result = {}
  for (const hash of hashes) {
    const prefix = hash.slice(0, prefixLength)
    if (result[prefix] === undefined) {
      result[prefix] = []
    }
    result[prefix].push(hash)
  }
  return result
}

const savePartitions = async (dir, partitionMap, hashType) => {
  const detaches = []
  const opObject = hashType === 'sha1' ? new OpenTimestamps.Ops.OpSHA1() : new OpenTimestamps.Ops.OpSHA256()
  for (const [prefix, items] of Object.entries(partitionMap)) {
    const data = Buffer.from(items.join(''), 'hex')
    fs.writeFileSync(path.join(dir, prefix), data)
    const hash = createHash('sha256')
    hash.update(data)
    const digest = hash.digest()
    const detach = OpenTimestamps.DetachedTimestampFile.fromHash(opObject, digest)
    detaches.push(detach)
  }
  await OpenTimestamps.stamp(detaches)
  const detachesSerialized = detaches.map(d => d.serializeToBytes())
  const prefixes = Object.keys(partitionMap)
  for (let i = 0; i < detachesSerialized.length; ++i) {
    fs.writeFileSync(path.join(dir, `${prefixes[i]}.ots`), detachesSerialized[i])
  }
}

const scanForInserts = async (path) => {
  let nextPosition = 0
  while (true) {
    const { position } = await findFirstInstance('/Volumes/Timestamper/libgen/libgen.sql', '\nINSERT ', { start: nextPosition })
    if (position < nextPosition) {
      break
    }
    const excerpt = (await readFilePart('/Volumes/Timestamper/libgen/libgen.sql', position, 200)).toString()
    console.log(excerpt)
    nextPosition = position + 1
  }
}
