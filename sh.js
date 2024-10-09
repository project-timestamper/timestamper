import fs from 'node:fs'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { findFirstInstance, readFilePart } from './util'

installIntoGlobal()

const SQL_ESCAPED_CHARS = {
  '\\\\0': '_0',
  '\\b': '\b',
  '\b': '_b',
  '\\t': '_tab_',
  '\\n': '\n',
  '\\r': '\r',
  '\\\\Z': '_Z_',
  '\\"': '"',
  "\\\\\\'": '_tick_'
}

const unescape = (s) => {
  // console.log(s)
  return SQL_ESCAPED_CHARS[s] || s
}

const ESCAPED_CHARS = new RegExp('\\\'|\b|\\x1A|\\\\0|\\t|\\n|\\r|\\\\Z|\\\\', 'g')

// f = '/Volumes/Timestamper/libgen/rar_files/backup_libgen_scimag.sql'

const getInsertLines = async function * (path, tableName) {
  let nextStart = 0
  while (true) {
    const { position } = await findFirstInstance(path, `INSERT INTO \`${tableName}\``, { start: nextStart })
    if (position < 0) {
      break
    }
    const { position: end, buf } = await findFirstInstance(path, ';\r\n', { start: position, accumulate: true })
    nextStart = end + 1
    yield buf
  }
}

const getHeader = async function (path, tableName) {
  const { position } = await findFirstInstance(path, `CREATE TABLE \`${tableName}\``)
  if (position < 0) {
    return null
  }
  const { buf } = await findFirstInstance(path, '\n)', { start: position, accumulate: true })
  const lines = buf.toString().split('\r\n')
  const results = []
  for (const line of lines) {
    const match = line.match(/^ {2}`([^\s]+)`/)
    if (match) {
      if (match[1]) {
        results.push(match[1])
      }
    }
  }
  return results
}

const partAfter = (x, s) => {
  const pos = x.indexOf(s)
  return x.substr(pos + s.length)
}

let bad, bad0
let latestLine

const getData = async function * (path, tableName) {
  const header = await getHeader(path, tableName)
  const lines = await getInsertLines(path, tableName)
  //  console.log(header)
  for await (const line of lines) {
    const lineString = line.toString()
    latestLine = lineString
    const rows = partAfter(lineString, ' VALUES ')
    // .replaceAll(ESCAPED_CHARS, unescape)
    const rows2 = rows.replaceAll('\f', '_f').replaceAll(/[\x00-\x1F]/g, 'x').replaceAll("','", '","').replaceAll('\\\\', '____').replaceAll('\\\\\\\'', '_____').replaceAll('\\\\\'', '_____\'').replaceAll('\\\'', '_').replaceAll('\\"', '_').replaceAll('\\', '__').replaceAll('\b', '_b').replaceAll('\t', '_tab_').replaceAll(/\)$/g, ']').replaceAll(/^\(/g, '[').replaceAll('),(', '],[').replaceAll("'", '"').replaceAll(',NULL,', ',null,')
    // console.log(rows2.substring(0, 100))
    // console.log(rows2.substr(rows2.length - 100, 1000))
    // console.log(rows2)
    let rows3
    try {
      rows3 = JSON.parse('[' + rows2 + ']')
    } catch (e) {
      bad0 = rows
      bad = rows2
      console.log(e)
      throw e
    }
    const final = rows3.map(row => {
      const result = {}
      for (let i = 0; i < header.length; ++i) {
        result[header[i]] = row[i]
      }
      return result
    })
    for await (const item of final) {
      yield item
    }
  }
}

const concat = async function * (arrays) {
  for await (const array of arrays) {
    for (const item of array) {
      yield item
    }
  }
}

const extractColumn = async function (path, tableName, colName) {
  const data1 = await getData(path, tableName)
  const data2 = await concat(data1)
  const data3 = data2.map(x => x[colName])
  const fh = await fs.promises.open(path + `_${colName}.txt`, 'w')
  for await (const data of data3) {
    console.log(data)
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

const scanForString = async (path, s, n = 200) => {
  let nextPosition = 0
  while (true) {
    const { position } = await findFirstInstance(path, s, { start: nextPosition })
    if (position < nextPosition) {
      break
    }
    const excerpt = (await readFilePart(path, position, n)).toString()
    console.log(position, excerpt)
    nextPosition = position + 1
  }
}
