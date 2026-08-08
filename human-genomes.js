import { createHash } from 'node:crypto'
import { once } from 'node:events'
import fs from 'node:fs'
import { createGunzip } from 'node:zlib'
import minimist from 'minimist'
import esMain from 'es-main'

const ROOT_URLS = [
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/integrated_sv_map/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/historical_data/',
  'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/changelog_details/'
]
const FTP_BASE = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'
const USER_AGENT = 'timestamper/0.0.1 (https://github.com/arthuredelstein/timestamper)'
const DEFAULT_OUTPUT = 'human_genome_hashes.txt'
const HASH_RETRIES = 5
const RETRY_DELAY_MS = 10000

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const sidecarPath = (outputPath, suffix) => {
  if (outputPath.endsWith('_hashes.txt')) {
    return outputPath.replace(/_hashes\.txt$/, `_${suffix}.txt`)
  }
  return `${outputPath}.${suffix}`
}

const normalizeDirUrl = (url) => (url.endsWith('/') ? url : `${url}/`)

const listDirectory = async (dirUrl, rootUrl) => {
  const root = normalizeDirUrl(rootUrl)
  const response = await fetch(normalizeDirUrl(dirUrl), {
    headers: { 'User-Agent': USER_AGENT }
  })
  if (!response.ok) {
    throw new Error(`list failed: ${response.status} ${dirUrl}`)
  }
  const html = await response.text()
  const hrefs = [...html.matchAll(/href="([^"]+)"/g)].map(match => match[1])
  const dirs = []
  const files = []

  for (const href of hrefs) {
    if (!href || href.startsWith('?') || href.startsWith('#') || href.startsWith('../')) {
      continue
    }

    const absolute = new URL(href, normalizeDirUrl(dirUrl))
    if (!absolute.href.startsWith(root)) {
      continue
    }

    if (href.endsWith('/')) {
      dirs.push(absolute.href)
    } else {
      files.push(absolute.href)
    }
  }

  return { dirs: [...new Set(dirs)], files: [...new Set(files)] }
}

const spiderFiles = async (rootUrls = ROOT_URLS) => {
  const files = []
  const seenDirs = new Set()

  for (const rootUrl of rootUrls) {
    const root = normalizeDirUrl(rootUrl)
    const queue = [root]
    console.log('spider root', root)

    while (queue.length > 0) {
      const dirUrl = queue.shift()
      if (seenDirs.has(dirUrl)) {
        continue
      }
      seenDirs.add(dirUrl)
      console.log('listing', dirUrl)
      const { dirs, files: leafFiles } = await listDirectory(dirUrl, root)
      files.push(...leafFiles)
      for (const dir of dirs) {
        if (!seenDirs.has(dir)) {
          queue.push(dir)
        }
      }
    }
  }

  return [...new Set(files)].sort()
}

const relativeKey = (fileUrl) => {
  if (fileUrl.startsWith(FTP_BASE)) {
    return fileUrl.slice(FTP_BASE.length)
  }
  return fileUrl
}

const pathnameOf = (url) => {
  try {
    return new URL(url).pathname
  } catch {
    return url
  }
}

const isGzipUrl = (url) => pathnameOf(url).endsWith('.gz')

const isVcfUrl = (url) => /\.vcf(\.gz)?$/i.test(pathnameOf(url))

const createHeaderScanner = () => {
  let buf = ''
  let finished = false
  let reference = null
  const sources = []

  return {
    get reference () {
      return reference
    },
    get sources () {
      return sources
    },
    get finished () {
      return finished
    },
    push (chunk) {
      if (finished) {
        return
      }
      buf += chunk.toString('utf8')
      const lines = buf.split(/\r?\n/)
      buf = lines.pop()
      for (const line of lines) {
        if (line.startsWith('##reference=')) {
          reference = line.slice('##reference='.length).trim()
        }
        if (line.startsWith('##source=')) {
          sources.push(line.slice('##source='.length).trim())
        }
        if (
          line.startsWith('#CHROM') ||
          (line.startsWith('#') && !line.startsWith('##')) ||
          (line.length > 0 && !line.startsWith('#'))
        ) {
          finished = true
          buf = ''
          return
        }
      }
      if (buf.length > 1_000_000) {
        finished = true
        buf = ''
      }
    }
  }
}

const efetchFastaUrl = (id) =>
  'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?' +
  new URLSearchParams({
    db: 'nuccore',
    id,
    rettype: 'fasta',
    retmode: 'text'
  }).toString()

const normalizeFtpHttpUrl = (candidate) =>
  candidate
    .replace(/^ftp:\/\//i, 'https://')
    .replace(/^(https:\/\/[^/]+)\/+/i, '$1/')

const resolveLocalFastaPath = (value) => {
  const pathMatch = value.match(/(\/?(?:[\w.-]+\/)*[\w.-]+\.(?:fa|fasta|fna)(?:\.gz)?)/i)
  if (!pathMatch) {
    return null
  }
  const filePath = pathMatch[1]
  if (filePath.startsWith('/vol1/ftp/')) {
    return `https://ftp.1000genomes.ebi.ac.uk${filePath}`
  }
  const base = filePath.split('/').pop()
  const withGz = base.endsWith('.gz') ? base : `${base}.gz`
  return `${FTP_BASE}technical/reference/${withGz}`
}

const resolveReferenceUrl = (value) => {
  if (!value) {
    return null
  }
  const trimmed = value.trim()
  const urlCandidate = trimmed.split(/\s+/)[0]
  if (/^https?:\/\//i.test(urlCandidate) || /^ftp:\/\//i.test(urlCandidate)) {
    return normalizeFtpHttpUrl(urlCandidate)
  }

  const refMatch = trimmed.match(/ref\|([A-Z]{1,2}_[0-9]+\.[0-9]+)\|/i)
  if (refMatch) {
    return efetchFastaUrl(refMatch[1])
  }

  const accessionMatch = trimmed.match(/\b([A-Z]{1,2}_[0-9]+\.[0-9]+)\b/)
  if (accessionMatch) {
    return efetchFastaUrl(accessionMatch[1])
  }

  const giMatch = trimmed.match(/gi\|([0-9]+)\|/i)
  if (giMatch) {
    return efetchFastaUrl(giMatch[1])
  }

  return resolveLocalFastaPath(trimmed)
}

const extractSourceFetchUrls = (value) => {
  if (!value) {
    return []
  }
  const urls = []
  const seen = new Set()
  const add = (url) => {
    if (url && !seen.has(url)) {
      seen.add(url)
      urls.push(url)
    }
  }

  for (const match of value.matchAll(/(?:https?|ftp):\/\/[^\s<>"']+/gi)) {
    add(normalizeFtpHttpUrl(match[0].replace(/[),.;]+$/g, '')))
  }

  for (const match of value.matchAll(/\/vol1\/ftp\/[^\s<>"']+/g)) {
    add(`https://ftp.1000genomes.ebi.ac.uk${match[0].replace(/[),.;]+$/g, '')}`)
  }

  // Tool paths / binary-looking filenames embedded in the source string
  for (const match of value.matchAll(
    /(?:\/[\w.-]+)+\/[\w.-]+|(?:[\w.-]+\.(?:jar|py|pl|sh|R|exe|fa|fasta|fna|vcf|bam|cram)(?:\.gz)?)/g
  )) {
    const token = match[0]
    if (/^https?:|^ftp:/i.test(token)) {
      continue
    }
    if (token.startsWith('/vol1/ftp/')) {
      add(`https://ftp.1000genomes.ebi.ac.uk${token}`)
      continue
    }
    if (/\.(?:fa|fasta|fna)(?:\.gz)?$/i.test(token)) {
      add(resolveLocalFastaPath(token))
    }
  }

  return urls
}

const hashStream = async (body, { gunzip = false, scanHeader = false } = {}) => {
  const hash = createHash('sha256')
  const scanner = scanHeader ? createHeaderScanner() : null

  const update = (chunk) => {
    hash.update(chunk)
    if (scanner) {
      scanner.push(chunk)
    }
  }

  if (!gunzip) {
    for await (const chunk of body) {
      update(chunk)
    }
    return {
      digest: hash.digest('hex'),
      reference: scanner?.reference ?? null,
      sources: scanner?.sources ?? []
    }
  }

  const unzip = createGunzip()
  let resolveDigest
  let rejectDigest
  const digestPromise = new Promise((resolve, reject) => {
    resolveDigest = resolve
    rejectDigest = reject
  })

  unzip.on('data', (chunk) => {
    update(chunk)
  })
  unzip.once('error', rejectDigest)
  unzip.once('end', () => {
    resolveDigest(hash.digest('hex'))
  })

  try {
    for await (const chunk of body) {
      if (!unzip.write(chunk)) {
        await once(unzip, 'drain')
      }
      // Keep reading the full VCF for hashing; header fields are only scanned near the top.
    }
    unzip.end()
    const digest = await digestPromise
    return {
      digest,
      reference: scanner?.reference ?? null,
      sources: scanner?.sources ?? []
    }
  } catch (e) {
    unzip.destroy()
    digestPromise.catch(() => {})
    throw e
  }
}

const hashFileOnce = async (url, { scanHeader = false } = {}) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }

  return hashStream(response.body, {
    gunzip: isGzipUrl(url),
    scanHeader
  })
}

const peekVcfHeaderOnce = async (url) => {
  const response = await fetch(url, { headers: { 'User-Agent': USER_AGENT } })
  if (!response.ok) {
    throw new Error(`status: ${response.status} ${url}`)
  }
  if (!response.body) {
    throw new Error(`no body: ${url}`)
  }

  const scanner = createHeaderScanner()
  const gunzip = isGzipUrl(url) ? createGunzip() : null

  const feed = (chunk) => {
    scanner.push(chunk)
  }

  try {
    if (!gunzip) {
      for await (const chunk of response.body) {
        feed(chunk)
        if (scanner.finished) {
          break
        }
      }
    } else {
      gunzip.on('data', feed)
      const errorPromise = new Promise((resolve, reject) => {
        gunzip.once('error', reject)
        gunzip.once('end', resolve)
      })
      for await (const chunk of response.body) {
        if (!gunzip.write(chunk)) {
          await once(gunzip, 'drain')
        }
        if (scanner.finished) {
          break
        }
      }
      gunzip.end()
      await errorPromise.catch(() => {})
    }
  } finally {
    if (typeof response.body.cancel === 'function') {
      try {
        await response.body.cancel()
      } catch {
        // ignore cancel errors after partial read
      }
    }
    if (gunzip) {
      gunzip.destroy()
    }
  }

  return {
    reference: scanner.reference,
    sources: scanner.sources
  }
}

const withRetries = async (label, fn) => {
  let lastError
  for (let attempt = 1; attempt <= HASH_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (e) {
      lastError = e
      const cause = e.cause ? ` (${e.cause.message || e.cause})` : ''
      console.error(`retry ${attempt}/${HASH_RETRIES}`, label, `${e.message}${cause}`)
      if (attempt < HASH_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt)
      }
    }
  }
  throw lastError
}

const hashFile = async (url, options) => withRetries(url, () => hashFileOnce(url, options))

const peekVcfHeader = async (url) => withRetries(url, () => peekVcfHeaderOnce(url))

const loadLineSet = (filePath) => {
  const values = new Set()
  if (!fs.existsSync(filePath)) {
    return values
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
        const value = line.trim()
        if (value) {
          values.add(value)
        }
      }
    }
    if (leftover.trim()) {
      values.add(leftover.trim())
    }
  } finally {
    fs.closeSync(fd)
  }
  return values
}

const appendLineOnce = (filePath, value, seen) => {
  if (!value || seen.has(value)) {
    return false
  }
  seen.add(value)
  fs.appendFileSync(filePath, `${value}\n`)
  return true
}

const loadDoneKeys = (filePath) => {
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
        const [key, digest] = line.split('\t')
        if (key && digest) {
          done.add(key)
        }
      }
    }
    if (leftover) {
      const [key, digest] = leftover.split('\t')
      if (key && digest) {
        done.add(key)
      }
    }
  } finally {
    fs.closeSync(fd)
  }
  return done
}

const appendHash = (filePath, key, digest) => {
  fs.appendFileSync(filePath, `${key}\t${digest}\n`)
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

const noteHeader = (extraUrls, extraUrlsPath, scannedPath, scannedKeys, sourceKey, { reference, sources }) => {
  appendLineOnce(scannedPath, sourceKey, scannedKeys)

  if (!reference) {
    console.log('reference', sourceKey, '(none)')
  } else {
    console.log('reference', sourceKey, reference)
    const url = resolveReferenceUrl(reference)
    if (url) {
      console.log('reference-url', sourceKey, url)
      appendLineOnce(extraUrlsPath, url, extraUrls)
    } else {
      console.log('reference-unfetched', sourceKey, reference)
    }
  }

  if (!sources || sources.length === 0) {
    console.log('source', sourceKey, '(none)')
    return
  }

  for (const source of sources) {
    console.log('source', sourceKey, source)
    const urls = extractSourceFetchUrls(source)
    if (urls.length === 0) {
      console.log('source-unfetched', sourceKey, source)
      continue
    }
    for (const url of urls) {
      console.log('source-url', sourceKey, url)
      appendLineOnce(extraUrlsPath, url, extraUrls)
    }
  }
}

export const collectHumanGenomeHashes = async (outputPath = DEFAULT_OUTPUT) => {
  const extraUrlsPath = sidecarPath(outputPath, 'extra_urls')
  const scannedPath = sidecarPath(outputPath, 'header_scanned')

  const done = loadDoneKeys(outputPath)
  const extraUrls = loadLineSet(extraUrlsPath)
  const scannedKeys = loadLineSet(scannedPath)
  console.log('already hashed:', done.size)
  console.log('cached extra urls:', extraUrls.size)
  console.log('headers scanned:', scannedKeys.size)

  const files = await spiderFiles(ROOT_URLS)
  const todoFiles = files.filter(url => !done.has(relativeKey(url)))
  console.log('listed:', files.length, 'todo:', todoFiles.length)

  const start = Date.now()
  let hashed = 0
  let attempted = 0
  let skipped = files.length - todoFiles.length

  // One-time header peek for VCFs hashed before header metadata was persisted.
  for (const url of files) {
    if (!isVcfUrl(url)) {
      continue
    }
    const key = relativeKey(url)
    if (!done.has(key) || scannedKeys.has(key)) {
      continue
    }
    try {
      const header = await peekVcfHeader(url)
      noteHeader(extraUrls, extraUrlsPath, scannedPath, scannedKeys, key, header)
    } catch (e) {
      console.error('header-peek', key, e.message)
    }
  }

  for (const url of todoFiles) {
    const key = relativeKey(url)
    attempted++
    try {
      const scanHeader = isVcfUrl(url)
      const { digest, reference, sources } = await hashFile(url, { scanHeader })
      appendHash(outputPath, key, digest)
      done.add(key)
      hashed++
      console.log(key, digest)
      if (scanHeader) {
        noteHeader(extraUrls, extraUrlsPath, scannedPath, scannedKeys, key, { reference, sources })
      }
      if (hashed % 10 === 0) {
        const elapsed = Date.now() - start
        const remaining = todoFiles.length - attempted
        const eta = attempted > 0 ? remaining * (elapsed / attempted) : 0
        console.log(
          `progress: ${hashed} hashed, ${attempted}/${todoFiles.length} attempted, ${skipped} skipped, ` +
          `elapsed ${formatDuration(elapsed)}, eta ${formatDuration(eta)}`
        )
      }
    } catch (e) {
      console.error(key, e.message)
    }
  }

  const extraList = [...extraUrls].sort()
  console.log('extra files to hash:', extraList.length)
  for (const url of extraList) {
    const key = relativeKey(url)
    if (done.has(key)) {
      console.log('extra-skip', key)
      continue
    }
    try {
      const { digest } = await hashFile(url, { scanHeader: false })
      appendHash(outputPath, key, digest)
      done.add(key)
      hashed++
      console.log('extra-hash', key, digest)
    } catch (e) {
      console.error('extra-hash', key, e.message)
    }
  }

  return hashed
}

const main = async () => {
  const args = minimist(process.argv.slice(2), {
    default: { output: DEFAULT_OUTPUT },
    alias: { o: 'output' }
  })

  const hashed = await collectHumanGenomeHashes(args.output)
  console.log('hashed this run:', hashed)
}

if (esMain(import.meta)) {
  main()
}
