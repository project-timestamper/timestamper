import { createHash } from 'node:crypto'
import DetachedTimestampFile from 'opentimestamps/src/detached-timestamp-file.js'

const collectionPrefixLengths = {
  libgen_fiction: 3,
  libgen_nonfiction: 3,
  tpb_movies: 3,
  yts_movies: 3,
  gutenberg_books: 2
}

const fetchBuffer = async (url) => {
  const response = await fetch(url)
  if (response.status >= 400) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  const arrayBuffer = await response.arrayBuffer()
  return Buffer.from(arrayBuffer)
}

const sha256 = (buf) => {
  const hash = createHash('sha256')
  hash.update(buf)
  return hash.digest()
}

const getSubsetBuffer = async (collection, hashBuf) => {
  const hashString = hashBuf.toString('hex')
  const prefixLength = collectionPrefixLengths[collection]
  const prefix = hashString.slice(0, prefixLength).toUpperCase()
  if (prefix === undefined) {
    throw new Error('no prefixLength found for \'collection\'')
  }
  const hashFile = `https://arthuredelstein.github.io/timestamper/${collection}/${prefix}`
  const subsetBuf = await fetchBuffer(hashFile)
  console.log(subsetBuf)
  if (subsetBuf.indexOf(hashBuf) === -1) {
    throw new Error(`hash ${hashString} not found`)
  }
  return subsetBuf
}

const getOtsFileForSubsetBuffer = async (subsetBuf) => {
  const subsetDigest = sha256(subsetBuf)
  const otsBuf = await fetchBuffer(hashFile + '.ots')
  const detachedOts = DetachedTimestampFile.deserialize(otsBuf)
  const otsFileDigest = Buffer.from(detachedOts.fileDigest())
  if (!otsFileDigest.equals(subsetDigest)) {
    throw new Error("subset file digest doesn't match ots file digest")
  }
  return detachedOts
}

const getOtsFile = async (collection, hashBuf) => {
  const subsetBuf = await getSubsetBuffer(collection, hashBuf)
  return await getOtsFileForSubsetBuffer(subsetBuf)
}
