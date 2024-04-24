const fs = require('node:fs')
const fsPromises = require('node:fs/promises')
const minimist = require('minimist')
const path = require('node:path')
const { execSync } = require('node:child_process')

const LOCAL_BASE = '/public/dumps/public/'

const getItems = async (basePath) => {
  try {
    const names = await fsPromises.readdir(basePath, { recursive: false })
    return names.map(name => path.join(basePath, name))
  } catch (e) {
    return []
  }
}

const getDateDirs = async (date) => {
  const entityDirs = await getItems(LOCAL_BASE)
  const wikiDirs = entityDirs.filter(dir => dir.endsWith('wiki'))
  const wikiDatesList = await Promise.all(wikiDirs.map(getItems))
  return wikiDatesList
    .map(list => (list ?? []).filter(dateDir => dateDir.endsWith(date))[0])
    .filter(x => x !== undefined)
}

const getImportantFiles = async (dir) => {
  const files = await getItems(dir)
  return files.filter(f => f.endsWith('-pages-articles.xml.bz2'))
}

const fileSHA256 = async (file) => {
  return execSync(`sha256sum ${path.resolve(file)}`).toString().split(' ')[0]
}

const getAllShaResults = async (date) => {
  const dateDirs = await getDateDirs(date)
  const allImportantFiles = (await Promise.all(dateDirs.map(getImportantFiles))).map(x => x[0]).map(x => x)
  const results = {}
  for (const importantFile of allImportantFiles) {
    const digest = await fileSHA256(importantFile)
    console.log(importantFile, digest)
    const shortPath = importantFile.replace(/^\/public\/dumps\/public\//, '')
    results[shortPath] = digest
  }
  return results
}

const writeResults = async (date, results, file) => {
  const data = {
    snapshot: date,
    files: results,
    digest: 'SHA-256',
    service: 'timestamper',
    source: 'https://github.com/arthuredelstein/timestamper',
    local_base: LOCAL_BASE,
    base: ['https://dumps.wikimedia.org/', 'https://dumps.wikimedia.your.org/']
  }
  await fsPromises.writeFile(file, JSON.stringify(data))
}

const timestamp = (file) => {
  const result = execSync(`npx opentimestamps stamp ${file}`).toString()
  console.log(result)
  return `${file}.ots`
}

const runDate = async (date) => {
  const outputFile = path.resolve(`../public_html/data/wikimedia_digests_${date}.json`)
  if (fs.existsSync(outputFile)) {
    console.log(`${outputFile} already exists; skipping.`)
    return
  }
  const results = await getAllShaResults(date)
  await writeResults(date, results, outputFile)
  timestamp(outputFile)
}

const strToDate = (s) => {
  const year = s.substring(0, 4)
  const month = s.substring(4, 6)
  const day = s.substring(6, 8)
  return new Date(year, month - 1, day)
}

const latestDumpDate = async () => {
  const enwikiDir = LOCAL_BASE + 'enwiki/'
  const dateDirs = await getItems(enwikiDir)
  const dateDirsTrimmed = dateDirs.filter(x => !x.endsWith('latest'))
  const mostRecentDateDir = dateDirsTrimmed[dateDirsTrimmed.length - 1]
  return mostRecentDateDir.replace(enwikiDir, '')
}

const timeElapsed = (d1, d2) => (d2 - d1) / (1000 * 60 * 60 * 24)

const main = async (dates) => {
  if (dates === undefined || dates.length === 0) {
    const dumpDate = await latestDumpDate()
    if (timeElapsed(strToDate(dumpDate), new Date()) > 4) {
      await runDate(dumpDate)
    }
  } else {
    for (const date of dates) {
      await runDate(date)
    }
  }
}

if (require.main === module) {
  const { _: dates } = minimist(process.argv.slice(2))
  main(dates)
}
