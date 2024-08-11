const fs = require('node:fs')
const { MerkleTree } = require('merkletreejs')
const { createHash } = require('node:crypto')

const sha256 = (x) =>
  createHash('sha256').update(x).digest()

const getPage = async (n) => {
  const response = await fetch(`https://yts.mx/api/v2/list_movies.json?page=${n}&limit=50`)
  return response.json()
}

const getMovies = async () => {
  const results = []
  for (let i = 0; ; ++i) {
    console.log(i)
    const page = await getPage(i)
    const movies = page.data.movies
    if (movies === undefined) {
      break
    }
    results.push(...movies)
  }
  return results
}

const formats = (movies) => {
  let total_bytes = 0
  const all_formats = []
  for (const { title_long, imdb_code, torrents } of movies) {
    const format = { title_long, imdb_code }
    for (const { hash, quality, size_bytes } of torrents) {
      all_formats.push({ ...format, quality, hash })
      total_bytes += size_bytes
    }
  }
  return { total_bytes, all_formats }
}

const movieMerkle = (leaves) => {
  const tree = new MerkleTree(leaves, sha256, { hashLeaves: false })
  return tree
}
