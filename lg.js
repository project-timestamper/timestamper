import fs from 'node:fs'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { stampAndSaveHashes } from './timestamp'
import { findFirstInstance } from './util'

installIntoGlobal()

const getInsertLines = async function * (path) {
  let nextStart = 0
  while (true) {
    const { position } = await findFirstInstance(path, 'INSERT INTO `fiction_hashes`', { start: nextStart })
    if (position < 0) {
      break
    }
    const { position: end, buf } = await findFirstInstance(path, ';\n', { start: position, accumulate: true })
    nextStart = end + 1
    yield buf
  }
}

const getData = async function * (path) {
  const lines = await getInsertLines(path)
  for await (const line of lines) {
    const lineString = line.toString()
    const [header, rows] = lineString.split(' VALUES ')
    const rows2 = rows.replaceAll('(', '[').replaceAll(')', ']').replaceAll('\'', '"')
    const rows3 = JSON.parse('[' + rows2 + ']')
    const header2 = header.split('(')[1]
    const header3 = '[' + header2.replaceAll('`', '"').replaceAll(')', ']')
    const header4 = JSON.parse(header3)
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

const extractSha256 = async function (path) {
  const data1 = await getData(path)
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
