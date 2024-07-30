const crypto = require('node:crypto')

const fetchBookHash = async (index) => {
  const response = await fetch(`https://www.gutenberg.org/cache/epub/${index}/pg${index}-h.zip`)
  if (response.status !== 200) {
    throw new Error(`status: ${response.status}`)
  }
  const stream = response.body
  const hash = crypto.createHash('sha256')
  for await (const chunk of stream) {
    hash.update(chunk)
  }
  return hash.digest('hex')
}
