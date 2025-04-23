import { createGunzip } from 'zlib'
import https from 'https'

async function getGzippedS3Stream (bucket, key) {
  const url = `https://${bucket}.s3.us-west-2.amazonaws.com/${key}`
  
  return new Promise((resolve, reject) => {
    https.get(url, response => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to get ${url}: ${response.statusCode}`))
        return
      }
      resolve(response.pipe(createGunzip()))
    }).on('error', reject)
  })
}

// Example usage:
async function main () {
  try {
    const stream = await getGzippedS3Stream(
      'discogs-data-dumps',
      'data/2025/discogs_20250401_masters.xml.gz'
    )

    // Example: pipe to process.stdout or handle the stream as needed
    stream.on('data', chunk => {
      console.log(chunk.toString())
    })

    stream.on('error', error => {
      console.error('Error reading stream:', error)
    })

    stream.on('end', () => {
      console.log('Finished reading stream')
    })
  } catch (error) {
    console.error('Error:', error)
  }
}

// Only run if this is the main module
if (process.argv[1] === new URL(import.meta.url).pathname) {
  main()
}

export { getGzippedS3Stream }