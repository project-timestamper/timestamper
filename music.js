import 'es-iterator-helpers/auto'
import Database from 'better-sqlite3'
import { makePartitions } from './partition.js'
import esMain from 'es-main'

const dbPath = '../annas_archive_spotify_2025_07_metadata/spotify_clean_track_files.sqlite3'
const db = new Database(dbPath, {
  readonly: true
})

const getHashes = function * () {
  const hashObjects = db.prepare('SELECT sha256_original FROM track_files').iterate()
  let count = 0
  for (const hashObject of hashObjects) {
    const hash = hashObject.sha256_original
    if (hash !== null && hash !== undefined) {
      yield hash
    }
    count++
    if (count % 100000 === 0) {
      console.log(`Processed ${count} hashes`)
    }
  }
}

const run = () => {
  makePartitions('./docs/annas_music_with_embedded_meta', getHashes(), 4)
}

if (esMain(import.meta)) {
  run()
}
