const fsPromises = require('node:fs/promises');
const path = require('node:path');
const { execSync } = require('node:child_process');

const getItems = async (basePath) => {
  try {
    const names = await fsPromises.readdir(basePath, { recursive: false });
    return names.map(name => path.join(basePath, name));
  } catch (e) {
    return [];
  }
};

const getDateDirs = async (date) => {
  const entityDirs = await getItems("/public/dumps/public/");
  const wikiDirs = entityDirs.filter(dir => dir.endsWith('wiki'));
  const wikiDatesList = await Promise.all(wikiDirs.map(getItems))
  return wikiDatesList
	.map(list => (list ?? []).filter(dateDir => dateDir.endsWith(date))[0])
	.filter(x => x !== undefined);
};

const getImportantFiles = async (dir) => {
  const files = await getItems(dir);
  return files.filter(f => f.endsWith("-pages-articles.xml.bz2"));
};

const file_sha256 = async (file) => {
  return execSync(`sha256sum ${path.resolve(file)}`).toString().split(" ")[0];
};

const getAllShaResults = async (date) => {
  const dateDirs = await getDateDirs(date);
  const allImportantFiles = (await Promise.all(dateDirs.map(getImportantFiles))).map(x => x[0]).map(x => x);
  const results = {};
  for (importantFile of allImportantFiles) {
    const digest = await file_sha256(importantFile);
    console.log(importantFile, digest);
    results[importantFile] = digest;
  }
  return results;
};

const writeResults = async (date, results) => {
  const data = {
    files: results,
    digest: "SHA-256",
    source: "timestamper"
  };
  const file = `wikimedia_digests_${date}.json`;
  await fsPromises.writeFile(file, JSON.stringify(results));
  return file;
};  				     
      
const timestamp = (file) => {
  const result = execSync(`npx ots-cli.js stamp ${file}`).toString();
  console.log(result);
  result `${file}.ots`;
};
