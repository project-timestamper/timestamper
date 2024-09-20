import path from 'node:path'
import { createHash } from 'node:crypto'
import OpenTimestamps from 'opentimestamps'
import fs from 'node:fs'

export const partitionByPrefix = (hashes, prefixLength) => {
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

export const savePartitions = async (dir, partitionMap) => {
  const detaches = []
  const opObject = new OpenTimestamps.Ops.OpSHA256()
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

export const makePartitions = async (dir, hashes, prefixLength) => {
  const partitionMap = partitionByPrefix(hashes, prefixLength)
  await savePartitions(dir, partitionMap)
}
