import { createHash } from 'node:crypto'
import fs from 'node:fs'
import { createInterface } from 'node:readline'
import { Readable } from 'node:stream'
import { createGunzip } from 'node:zlib'
import minimist from 'minimist'
import esMain from 'es-main'

const SUMMARY_URLS = {
  refseq: 'https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt',
  genbank: 'https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt'
}

const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'genome_hashes.txt'

const genomicFnaUrl = (ftpPath) => {
  const dir = ftpPath.replace(/^ftp:/, 'https:').replace(/\/+$/, '')
  const basename = dir.split('/').pop()
  return `${dir}/${basename}_genomic.fna.gz`
}

const parseAssemblyLine = (line, source) => {
  if (!line || line.startsWith('#')) {
    return null
  }
  const cols = line.split('\t')
  const assemblyAccession = cols[0]
  const ftpPath = cols[19]

  if (!ftpPath || ftpPath === 'na') {
    return null
  }

  return {
    accession: assemblyAccession,
    url: genomicFnaUrl(ftpPath),
    source
  }
}

const listAssembliesFromSummary = async (url, source) => {
  console.log('fetching', source, url)
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`fetch failed: ${response.status} ${url}`)
  }

  const rl = createInterface({
    input: Readable.fromWeb(response.body),
    crlfDelay: Infinity
  })

  const assemblies = []
  for await (const line of rl) {
    const asm = parseAssemblyLine(line, source)
    if (asm) {
      assemblies.push(asm)
    }
  }
  console.log(source, assemblies.length)
  return assemblies
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

const listAssemblies = async () => {
  const lists = []
  for (const [source, url] of Object.entries(SUMMARY_URLS)) {
    lists.push(await listAssembliesFromSummary(url, source))
  }
  return lists.flat()
}

const loadDoneAccessions = (filePath) => {
  const done = new Set()
  if (!fs.existsSync(filePath)) {
    return done
  }
  const text = fs.readFileSync(filePath, 'utf8')
  for (const line of text.split('\n')) {
    if (!line) {
      continue
    }
    const [accession, digest] = line.split('\t')
    if (accession && digest) {
      done.add(accession)
    }
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

  const assemblies = (await listAssemblies()).filter(asm => !done.has(asm.accession))
  console.log('remaining:', assemblies.length)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  for (const asm of assemblies) {
    attempted++
    try {
      const digest = await hashGenome(asm.url)
      appendHash(outputPath, asm.accession, digest)
      hashed++
      console.log(asm.accession, digest)
      if (hashed % 10 === 0) {
        const elapsed = Date.now() - start
        const remaining = assemblies.length - attempted
        const eta = remaining * (elapsed / attempted)
        console.log(
          `progress: ${hashed} hashed, ${attempted}/${assemblies.length} attempted, ` +
          `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
        )
      }
    } catch (e) {
      console.error(asm.accession, e.message)
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
