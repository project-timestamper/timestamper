import { createHash } from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { createInterface } from 'node:readline'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { createGunzip } from 'node:zlib'
import minimist from 'minimist'
import esMain from 'es-main'

const SUMMARY_URLS = {
  refseq: 'https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt',
  genbank: 'https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt'
}

const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'genome_hashes.txt'
const SUMMARY_DIR = '/tmp/projecttimestamper'

const genomicFnaUrl = (ftpPath) => {
  const dir = ftpPath.replace(/^ftp:/, 'https:').replace(/\/+$/, '')
  const basename = dir.split('/').pop()
  return `${dir}/${basename}_genomic.fna.gz`
}

const parseAssemblyLine = (line) => {
  if (!line || line.startsWith('#')) {
    return null
  }
  const cols = line.split('\t')
  const accession = cols[0]
  const ftpPath = cols[19]

  if (!ftpPath || ftpPath === 'na') {
    return null
  }

  return {
    accession,
    url: genomicFnaUrl(ftpPath)
  }
}

const downloadSummary = async (url, destPath) => {
  await fs.promises.mkdir(path.dirname(destPath), { recursive: true })
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`fetch failed: ${response.status} ${url}`)
  }
  await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(destPath))
}

const countAssembliesInFile = async (filePath, done) => {
  const rl = createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  })

  let total = 0
  let todo = 0
  for await (const line of rl) {
    const asm = parseAssemblyLine(line)
    if (!asm) {
      continue
    }
    total++
    if (!done.has(asm.accession)) {
      todo++
    }
  }
  return { total, todo }
}

async function * iterateAssembliesFromFile (filePath) {
  const rl = createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  })

  for await (const line of rl) {
    const asm = parseAssemblyLine(line)
    if (asm) {
      yield asm
    }
  }
}

const hashGenome = async (url) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  const hash = createHash('sha256')
  const input = Readable.fromWeb(response.body).pipe(createGunzip())
  for await (const chunk of input) {
    hash.update(chunk)
  }
  return hash.digest('hex')
}

const loadDoneAccessions = (filePath) => {
  const done = new Set()
  if (!fs.existsSync(filePath)) {
    return done
  }
  const fd = fs.openSync(filePath, 'r')
  const bufSize = 1024 * 1024
  const buf = Buffer.alloc(bufSize)
  let leftover = ''
  try {
    while (true) {
      const bytesRead = fs.readSync(fd, buf, 0, bufSize, null)
      if (bytesRead === 0) {
        break
      }
      leftover += buf.toString('utf8', 0, bytesRead)
      const lines = leftover.split('\n')
      leftover = lines.pop()
      for (const line of lines) {
        if (!line) {
          continue
        }
        const [accession, digest] = line.split('\t')
        if (accession && digest) {
          done.add(accession)
        }
      }
    }
    if (leftover) {
      const [accession, digest] = leftover.split('\t')
      if (accession && digest) {
        done.add(accession)
      }
    }
  } finally {
    fs.closeSync(fd)
  }
  return done
}

const appendHash = (filePath, accession, digest) => {
  fs.appendFileSync(filePath, `${accession}\t${digest}\n`)
}

const formatDuration = (ms) => {
  const totalSec = Math.max(0, Math.round(ms / 1000))
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  if (h > 0) {
    return `${h}h ${m}m ${s}s`
  }
  if (m > 0) {
    return `${m}m ${s}s`
  }
  return `${s}s`
}

export const collectGenomeHashes = async (outputPath = DEFAULT_OUTPUT) => {
  const done = loadDoneAccessions(outputPath)
  console.log('already hashed:', done.size)

  const summaryPaths = []
  for (const [source, url] of Object.entries(SUMMARY_URLS)) {
    const summaryPath = path.join(SUMMARY_DIR, `assembly_summary_${source}.txt`)
    console.log('downloading', source, url)
    await downloadSummary(url, summaryPath)
    console.log('saved', summaryPath)
    summaryPaths.push([source, summaryPath])
  }

  let listed = 0
  let todo = 0
  for (const [source, summaryPath] of summaryPaths) {
    const counts = await countAssembliesInFile(summaryPath, done)
    console.log(source, 'listed', counts.total, 'todo', counts.todo)
    listed += counts.total
    todo += counts.todo
  }
  console.log('total listed:', listed, 'todo:', todo)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  let skipped = 0

  for (const [source, summaryPath] of summaryPaths) {
    console.log('hashing', source)
    for await (const asm of iterateAssembliesFromFile(summaryPath)) {
      if (done.has(asm.accession)) {
        skipped++
        continue
      }

      attempted++
      try {
        const digest = await hashGenome(asm.url)
        appendHash(outputPath, asm.accession, digest)
        done.add(asm.accession)
        hashed++
        console.log(asm.accession, digest)
        if (hashed % 10 === 0) {
          const elapsed = Date.now() - start
          const remaining = todo - attempted
          const eta = attempted > 0 ? remaining * (elapsed / attempted) : 0
          console.log(
            `progress: ${hashed} hashed, ${attempted}/${todo} attempted, ${skipped} skipped, ` +
            `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
          )
        }
      } catch (e) {
        console.error(asm.accession, e.message)
      }
    }
  }
  return hashed
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output' }
  })

  const hashed = await collectGenomeHashes(args.output)
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
