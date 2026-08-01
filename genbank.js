import { fetchDOM, fetchGzipAsLines } from './utils.js'
import crypto from 'crypto'
import fs from 'fs/promises'
import esMain from 'es-main'

const baseHref = 'https://ftp.ncbi.nlm.nih.gov/genbank/'

const bigSequences = {
  "pri": "primate sequences",
  "rod": "rodent sequences",
  "mam": "other mammalian sequences",
  "vrt": "other vertebrate sequences",
  "inv": "invertebrate sequences",
  "pln": "plant, fungal, and algal sequences",
  "bct": "bacterial sequences",
  "vrl": "viral sequences",
  "phg": "bacteriophage sequences",
  "htg": "HTGS sequences (High Throughput Genomic sequences)",
  "con": "Constructed sequences"
}

const dropUntil = (items, until) => {
  const result = []
  let drop = true
  for (const item of items) {
    if (item.includes(until)) {
      drop = false
    }
    if (!drop) {
      result.push(item)
    }
  }
  return result
}

const getGenbankUrls = async (after) => {
  const prefixes = Object.keys(bigSequences).map(x => "gb" + x)
  const dom = await fetchDOM(baseHref)
  const linkElements = [...dom.window.document.getElementsByTagName('a')]
  const gzFiles = linkElements.map(el => el.getAttribute('href')).filter(n => n.includes('.seq'))
  const gzFiles2 = gzFiles.filter(x => prefixes.includes(x.match(/gb[a-z]+/)[0]))
  const gzFiles3 = dropUntil(gzFiles2, after)
  return gzFiles2.map(n => baseHref + n)
}

const getReleaseNumber = async () => {
  const response = await fetch(baseHref + 'GB_Release_Number')
  const text = await response.text()
  return text.trim()
}


const hashEntries = async function* (lines) {
  console.log("hashEntries");
  let hash = crypto.createHash('sha256');
  for await (const line of lines) {
    console.log(line)
    hash.update(line + "\n")
    if (line === "//") {
      console.log("// found");
      yield hash.digest('hex');
      hash = crypto.createHash('sha256');
    }
  }
};

const processOneFile = async function * (url) {
  const lines = await fetchGzipAsLines(url)
  return await hashEntries(lines)
}

const processAllFiles = async(urls) => {
  const file = await fs.open("genbank_hashes.txt", 'a')
  for (const url of urls) {
    console.log(url)
    const hashes = await processOneFile(url)
    for await (const hash of hashes) {
      await file.write(hash + "\n")
    }
  }
  await file.close();
}

const main = async () => {
  const urls = await getGenbankUrls("gbinv1058")
    console.log(urls.slice(0,5));  
  await processAllFiles(urls)
}

if (esMain(import.meta)) {
  main()
}
