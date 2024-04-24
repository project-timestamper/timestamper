const fsPromises = require('node:fs/promises')
const path = require('node:path')
const { execSync } = require('node:child_process')
const minimist = require('minimist')

const getItems = async (basePath) => {
  try {
    const names = await fsPromises.readdir(basePath, { recursive: false })
    return names.map(name => path.join(basePath, name))
  } catch (e) {
    return []
  }
}

const getDateDirs = async (date) => {
  const entityDirs = await getItems('/public/dumps/public/')
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
    results[importantFile] = digest
  }
  return results
}

const writeResults = async (date, results) => {
  const data = {
    snapshot: date,
    files: results,
    digest: 'SHA-256',
    service: 'timestamper',
    source: 'https://github.com/arthuredelstein/timestamper'
  }
  const file = path.resolve(`../public_html/data/wikimedia_digests_${date}.json`)
  await fsPromises.writeFile(file, JSON.stringify(data))
  return file
}

const timestamp = (file) => {
  const result = execSync(`npx opentimestamps stamp ${file}`).toString()
  console.log(result)
  return `${file}.ots`
}

const runDate = async (date) => {
  const results = await getAllShaResults(date)
  const resultsFile = await writeResults(date, results)
  const otsFile = timestamp(resultsFile)
  console.log(otsFile)
}

const latestDumpDate = () => {
  return '20220101'
}

const main = async () => {
  const { _: dates } = minimist(process.argv.slice(2))
  if (dates === undefined || dates.length === 0) {
    const date = await latestDumpDate()
    await runDate(date)
  } else {
    for (const date of dates) {
      await runDate(date)
    }
  }
}

if (require.main === module) {
  main()
}
