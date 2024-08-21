import fs from 'node:fs'
import path from 'node:path'
import OpenTimestamps from 'opentimestamps'
import { createClient, putObject } from './r2.js'
import pMap from 'p-map'

const stampHashes = async (hashList, hashType) => {
  const opObject = hashType === 'sha1' ? new OpenTimestamps.Ops.OpSHA1() : new OpenTimestamps.Ops.OpSHA256()
  const detaches = hashList.map(hash => {
    const hexHash = Buffer.from(hash, 'hex')
    return OpenTimestamps.DetachedTimestampFile.fromHash(opObject, hexHash)
  })
  await OpenTimestamps.stamp(detaches)
  return detaches
}

const writeHashes = (hashList, detaches, outDir) => {
  const detachesSerialized = detaches.map(d => d.serializeToBytes())
  fs.mkdirSync(outDir, { recursive: true })
  for (const i in hashList) {
    const filename = path.join(outDir, `${hashList[i]}.ots`)
    fs.writeFileSync(filename, Buffer.from(detachesSerialized[i]))
    console.log(filename)
  }
}

const zipMap = (keys, values) => {
  const result = {}
  for (const i in keys) {
    result[keys[i]] = values[i]
  }
  return result
}

const stampAndWriteHashes = async (hashList, outDir) => {
  const detaches = await stampHashes(hashList)
  writeHashes(hashList, detaches, outDir)
}

const stampAndCollectHashes = async (hashList) => {
  const hashType = hashList[0].length === 40 ? 'sha1' : 'sha256'
  const detaches = (await stampHashes(hashList, hashType)).map(
    detach => detach.serializeToBytes())
  console.log(hashList.length, detaches.length)
  return zipMap(hashList, detaches)
}

const uploadHashes = async (hashPairs) => {
  let uploadCount = 0
  const client = createClient()
  await pMap(hashPairs,
    async ([hash, otsFile]) => {
      uploadCount++
      if (uploadCount % 1000 === 0) {
        console.log(uploadCount)
      }
      await putObject(client, `ots/${hash}.ots`, otsFile)
    },
    { concurrency: 64, stopOnError: false })
  client.destroy()
}

export const stampAndUploadHashes = async (hashList) => {
  const hashToDetachMap = await stampAndCollectHashes(hashList)
  await uploadHashes(Object.entries(hashToDetachMap))
}
