import { installIntoGlobal } from 'iterator-helpers-polyfill'
import { readJson } from './util'
import { S3Client, PutObjectCommand, CopyObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3'
import pMap from 'p-map'

installIntoGlobal()

const createClient = () => {
  const credentials = readJson('../cloudflare-access.json')
  return new S3Client({
    region: 'us-east-1',
    endpoint: `https://${credentials.cloudflareAccountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey
    }
  })
}

const putObject = async (client, path, body) => {
  const command = new PutObjectCommand({ Bucket: 'timestamper', Key: path, Body: body })
  return client.send(command)
}

const listObjects = async function * (client) {
  let ContinuationToken
  while (true) {
    const command = new ListObjectsV2Command(
      { Bucket: 'timestamper', ContinuationToken })
    const results = await client.send(command)
    const items = results.Contents
    for (const item of items) {
      yield item
    }
    if (results.NextContinuationToken) {
      ContinuationToken = results.NextContinuationToken
      console.log(`ContinuationToken='${ContinuationToken}'`)
    } else {
      break
    }
  }
}

const deleteObject = async (client, path) => {
  const command = new DeleteObjectCommand({ Bucket: 'timestamper', Key: path })
  return client.send(command)
}

const deleteObjects = async (client, paths) => {
  await pMap(paths, (path) => deleteObject(client, path),
    { concurrency: 64, stopOnError: false })
}

const copyObject = async (client, pathOld, pathNew) => {
  const command = new CopyObjectCommand({ Bucket: 'timestamper', CopySource: ('timestamper/' + pathOld), Key: pathNew })
  return client.send(command)
}

const copyObjects = async (client, pathPairs) => {
  await pMap(pathPairs,
    ([pathOld, pathNew]) => copyObject(client, pathOld, pathNew),
    { concurrency: 64, stopOnError: false })
}

module.exports = { createClient, putObject }
