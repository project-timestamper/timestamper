import crypto from 'crypto'
import { JSDOM } from 'jsdom'
import fs from 'fs/promises'
import esMain from 'es-main'
import { makePartitions } from './partition.js'

const baseUrl = 'https://www.wikiart.org'

const errorLog = await fs.open('errorLog.txt', 'w')

const paintingHashesFile = 'docs/painting-hashes.tsv'

const logError = (text, error) => {
  console.error(text, error)
  errorLog.write(text + ' -- ' + error.message + ': ' + error.stack + '\n')
}

const hashFile = async (url) => {
  const response = await fetch(url)
  const buffer = Buffer.from(await response.arrayBuffer())
  return crypto.createHash('sha256').update(buffer).digest('hex')
}

const fetchDOM = async (url) => {
  const response = await fetch(url)
  const text = await response.text()
  return new JSDOM(text)
}

const linksListedOnPage = async (url) => {
  const dom = await fetchDOM(url)
  const links = dom.window.document.querySelectorAll('main ul li a')
  return [...links].map(link => `${baseUrl}${link.href}`)
}

const artistsForLetter = async (firstLetter) => {
  const url = `${baseUrl}/en/Alphabet/${firstLetter}/text-list`
  return linksListedOnPage(url)
}

const allArtistPages = async () => {
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('')
  const names = await Promise.all(letters.map(artistsForLetter))
  return names.flat().sort()
}

const paintingsForArtist = async (url) =>
  linksListedOnPage(url + '/all-works/text-list')

const allPaintingPages = async function * () {
  const artists = await allArtistPages()
  for (const artist of artists) {
    try {
      const paintingPages = await paintingsForArtist(artist)
      console.log(artist, paintingPages.length)
      for (const paintingPage of paintingPages) {
        yield paintingPage
      }
    } catch (e) {
      logError(artist, e)
    }
  }
}

const originalImageUrlForPainting = async (paintingUrl) => {
  const dom = await fetchDOM(paintingUrl)
  const mainElement = dom.window.document.querySelector('section.wiki-layout-left-menu main')
  const thumbnailsJson = mainElement.getAttribute('ng-init').split(' = ')[1].replace(/;$/g, '')
  const thumbnails = JSON.parse(thumbnailsJson).ImageThumbnailsModel[0].Thumbnails
  const originalThumbnail = thumbnails.find(thumbnail => thumbnail.Name === 'Original')
  return originalThumbnail.Url
}

const getPaintingHashes = async (urls, outputFile) => {
  const file = await fs.open(outputFile, 'w')
  for await (const paintingPage of urls) {
    try {
      const imageUrl = await originalImageUrlForPainting(paintingPage)
      const hash = await hashFile(imageUrl)
      const line = `${paintingPage}\t${imageUrl}\t${hash}\n`
      console.log(line)
      file.write(line)
    } catch (e) {
      logError(paintingPage, e)
    }
  }
  file.close()
}

const allPaintingHashes = async (outputFile) => {
  const paintingPages = allPaintingPages()
  await getPaintingHashes(paintingPages, outputFile)
}

export const allPaintingUrls = async () => {
  const paintingPages = allPaintingPages()
  const file = fs.createWriteStream('painting-pages.tsv')
  for await (const paintingPage of paintingPages) {
    try {
      console.log(paintingPage)
      file.write(`${paintingPage}\n`)
    } catch (e) {
      logError(paintingPage, e)
    }
  }
  file.close()
}

export const readPaintingUrls = async () => {
  const urls = new Set()
  const file = await fs.open('painting-pages.tsv')
  for await (const line of file.readLines()) {
    urls.add(line)
  }
  return urls
}

export const findMissingUrls = async (expectedUrls) => {
  const file = await fs.open(paintingHashesFile)
  for await (const line of file.readLines()) {
    const foundUrl = line.split('\t')[0]
    expectedUrls.delete(foundUrl)
  }
  return expectedUrls
}

export const readHashes = async () => {
  const file = await fs.open(paintingHashesFile)
  const hashes = []
  for await (const line of file.readLines()) {
    const hash = line.split('\t')[2]
    hashes.push(hash)
  }
  return hashes
}

const main = async () => {
  await allPaintingHashes(paintingHashesFile)
}

if (esMain(import.meta)) {
  main()
  errorLog.close()
}
