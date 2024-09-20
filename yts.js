import { readJson } from './util.js'
import { stampAndUploadHashes } from './timestamp.js'
import fs from 'node:fs'
import { makePartitions } from './partition.js'

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

const readHashes = () => {
  const formats = readJson('docs/yts-movie-formats.json')
  return formats.map(f => f.hash)
}

const timestamp = async () => {
  const data = readJson('docs/yts-movie-formats.json')
  const hashes = data.map(item => item.hash)
  await stampAndUploadHashes(hashes)
}
