import fs from 'node:fs'
import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { stampAndSaveHashes } from './timestamp'
installIntoGlobal()

const strToDate = (s) => {
  const year = s.substring(0, 4)
  const month = s.substring(4, 6)
  const day = s.substring(6, 8)
  return new Date(year, month - 1, day)
}
export const readJson = (file) => JSON.parse(fs.readFileSync(file).toString())

export const findFirstInstance = async (path, content, { start, end, accumulate } = { start: 0, end: Infinity, accumulate: false }) => {
  let i = 0
  const pieces = []
  const stream = fs.createReadStream(path, { start, end })
  let tailPiece = Buffer.alloc(0)
  for await (const chunk of stream) {
    const examine = Buffer.concat([tailPiece, chunk])
    const loc = examine.indexOf(content)
    if (loc > -1) {
      const position = i + loc - tailPiece.length + start
      const result = { position }
      if (accumulate) {
        pieces.push(chunk.slice(0, loc - tailPiece.length))
        result.buf = Buffer.concat(pieces)
      }
      return result
    } else {
      if (accumulate) {
        pieces.push(chunk)
      }
    }
    i += chunk.length
    tailPiece = chunk.slice(chunk.length - content.length + 1)
  }
  return { position: -1 }
}

export const readFilePart = async (path, position, length) => {
  const fh = await fs.promises.open(path, 'r')
  const buffer = Buffer.alloc(length)
  await fh.read({ position, length, buffer })
  fh.close()
  return buffer
}
