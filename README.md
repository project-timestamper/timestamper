# Project Timestamper

## Overview

This repository contains the primary data files for Project Timestamper, an effort to preserve the integrity of humanity's cultural and scientific achievements.

The data consists of verifiable timestamp proofs for a number of collections of human works, including books, research papers, movies, paintings, music, genomes, patents, protein structures and web page captures. The proofs are arranged in small, flat, static files, in order to ensure that the verification process is quick, low bandwidth, and future-proof.

This repository includes no copyrighted content, files, or metadata: only cryptographic digests. A digest cannot be used to search for or reconstruct an original work; it can be used only to verify that a work you already possess existed on a certain date.

For more information on Project Timestamper, please see https://projecttimestamper.org.

## Description of collections

Each collection's set of proofs is in its own directory. These proofs are static files containing hash list files and an OpenTimestamps (`.ots`) attestation of those files.

| Collection | Item count | Hash algorithm | Bytes/hash | Prefix size (hex digits) | Timestamp date | Hash source |
|---|---|---|---|---|---|---|
| annas_archive_torrents | ~25K | SHA-1 | 20 | 2 | 2026-09-08 | Infohashes |
| annas_literature_hashes | ~17.0M | MD5 | 16 | 4 | 2026-09-09 | Torrent metadata |
| annas_music | ~86M | SHA-256 | 32 | 4 | 2025-12-27 | Source database |
| annas_music_with_embedded_meta | ~86M | SHA-256 | 32 | 4 | 2025-12-27 | Source database |
| arxiv_papers | ~7.29M | MD5 | 16 | 3 | 2026-10-01 | Source database |
| [common_crawl_blocks](https://github.com/project-timestamper/timestamper-commoncrawl) | ~134M | SHA-256 | 32 | — | 2026-09-26 | Computed |
| epo_patents | ~7.01M | SHA-256 | 32 | 3 | 2026-09-09 | Computed |
| gutenberg_books | ~72K | SHA-256 | 32 | 2 | 2024-09-19 | Computed |
| github_repos | ~2.87M | SHA-1 | 20 | 3 | 2026-10-10 | Commit digests |
| human_genome_variants | ~5.4K | SHA-256 | 32 | 1 | 2026-09-14 | Computed |
| libgen_fiction | ~3.03M | SHA-256 | 32 | 3 | 2024-09-16 | Source database |
| libgen_nonfiction | ~4.37M | SHA-256 | 32 | 3 | 2024-09-16 | Source database |
| ncbi_genomes | ~4.2M | SHA-256 | 32 | 3 | 2026-08-21 | Computed |
| pdb_files | ~4.73M | SHA-256 | 32 | 3 | 2026-09-26 | Computed |
| scihub_articles | ~85.1M | MD5 | 16 | 4 | 2024-10-11 | Source database |
| tpb_movies | ~822K | SHA-1 | 20 | 3 | 2024-09-19 | Infohashes |
| wikiart_works | ~192K | SHA-256 | 32 | 2 | 2025-02-27 | Computed |
| yts_movies | ~135K | SHA-1 | 20 | 3 | 2024-09-19 | Infohashes |

Hash source indicates how the digests were obtained: 
- *Computed*: digests computed by Project Timestamper
- *Source database*: digests computed by source
- *Infohashes*: digests extracted from torrent links
- *Torrent metadata*: content digests extracted from torrent file lists
- *Commit digests*: default-branch tip commit IDs (as of a published cutoff) from GitHub

In each case, hash lists containing these digests were the files submitted to OpenTimestamps for timestamping.

## Hash list layout

Digests of individual works are stored together in hash list files. In each hash list file, digests are concatenated in binary format. For example, a hash list file with 100 SHA-256 digests (each 32 bytes) would be 3200 bytes long.

For each collection, the set of digests is partitioned into subsets, by the prefix (first several bits) of each digest. The name of each hash list file is the prefix in hex:
```
docs/<collection>/<PREFIX>      # hash list file (binary format)
docs/<collection>/<PREFIX>.ots  # OpenTimestamps attestation of hash list file
```

For example, the `000` file in libgen_fiction contains a concatenation of digests, each of which begins with 12 zero bits.

## Verification

To manually verify that a work existed by the attested date, you can carry out the following steps:

1. Digest the work with that collection’s **Hash algorithm** (SHA-256, SHA-1, or MD5, depending on the collection)
2. Take the first **Prefix size** hex digits of the digest converted to uppercase, and load `docs/<collection>/<PREFIX>` (or fetch the hosted copy at `https://project-timestamper.github.io/timestamper/<collection>/<PREFIX>`). Also load the corresponding `<PREFIX>.ots` file.
3. Confirm that the hash file contains the digest as raw bytes, using a digest length given by the **Bytes/hash** column.
4. Verify the `.ots` proof against Bitcoin (for example `ots verify 000.ots`). Success proves that the hash list file, and therefore the work and its digest, existed by the attested block time.

Automated tools for verification are at https://github.com/project-timestamper/stamper.

## Common Crawl index proofs

Common Crawl’s monthly CDXJ index is too large to store digests in this repository. Those proofs live in a separate repo: [timestamper-commoncrawl](https://github.com/project-timestamper/timestamper-commoncrawl).

The Common Crawl CDXJ index is structured hierarchically. A single page capture is represented by an index line, containing a URL and a SHA-1 digest of the content. Up to 3000 index lines are concatenated together in a ZipNum block. A few thousand (on average) ZipNum blocks are concatenated in a gzipped shard file. Each crawl (representing captures obtained on a given month) contains 300 shards. At the time of writing, 128 crawls made up the whole collection.

As of 2026-09-26, the Common Crawl Index Statistics were as follows:
 * 128 total crawls
 * 300 shards (gzip files) per crawl (38,400 total shards)
 * 3503 ZipNum blocks per shard, on average (134,497,076 total ZipNum blocks)
 * 2527 page capture lines per ZipNum block, on average (339.8 billion total page capture lines)

(The corresponding full-page capture data (in WARC files) exceeded 10 PiB.)

Project Timestamper downloaded each `cdx-*.gz` shard file one by one, recording the SHA-256 of each ZipNum blocks in the shard, and collecting those digests in a binary hash list per shard (not prefix-partitioned), plus a corresponding `.ots` file. For example (within the timestamper-commoncrawl repository):

```
docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN      # ~2900 × 32-byte digests (~93 KB)
docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN.ots  # OpenTimestamps attestation
```

The hash list filename is the CDXJ `part` with `.gz` stripped (e.g. `cdx-00066.gz` → `cdx-00066`). Digests are concatenated in block order within that shard.

The CDXJ shards themselves stay on Common Crawl (`data.commoncrawl.org`). Block location at verify time comes from Common Crawl’s CDX API (`showPagedIndex`), so no large SURT→block locator needed to be hosted by us.

### Verification procedure

To verify that a URL’s capture was present in a given crawl’s index by the attested date:

1. Choose a crawl id (for example `CC-MAIN-2026-34`) and the URL of interest.
2. Locate the ZipNum block with one API request:
   ```
   GET https://index.commoncrawl.org/<CRAWL>-index?url=<URL>&output=json&showPagedIndex=true&page=0
   ```
   The JSON includes `part` (e.g. `cdx-00066.gz`), `offset`, and `length` (compressed byte range of that gzip member). The `urlkey` field is the **first** key in the block, not necessarily the query URL.
3. Download that block only (~280 KB typical; a few MB at most):
   ```
   GET https://data.commoncrawl.org/cc-index/collections/<CRAWL>/indexes/<part>
   Range: bytes=<offset>-<offset+length-1>
   ```
4. Compute `H = SHA-256` of the downloaded compressed bytes.
5. Load the shard hash list and attestation named from `part` (strip `.gz`):
   `docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN` and `cdx-NNNNN.ots`
   (or the hosted copies under `https://project-timestamper.github.io/timestamper-commoncrawl/common_crawl_blocks/<CRAWL>/…`).
   Confirm `H` is present as a raw 32-byte digest somewhere in that file, then verify the `.ots` proof of that file (for example `ots verify cdx-00066.ots`).
6. Gunzip the block and confirm it contains the expected CDXJ line (URL / digest).
7. Use the line’s WARC `filename` / `offset` / `length` for a second range request to `data.commoncrawl.org`, download the payload, hash it with SHA-1 and verify a match with the CDXJ `digest`.

Typical verify traffic is a few small requests on the order of **~0.4 MB** (hash list ~93 KB + block ~280 KB, excluding WARC payload).

## GitHub repo verification

The `github_repos` collection attests default-branch tip commits as of a cutoff time **D**, published in `docs/github_repos/github_until.txt`.

To verify that a repository’s history includes an attested tip:

1. Read **D** from `docs/github_repos/github_until.txt` (also attested as `github_until.txt.ots`).
2. Clone the repository and check out its default branch.
3. Find the most recent commit on that branch with committer date at or before **D**:
   ```
   git log -1 --before=<D> --format=%H
   ```
   Call that commit digest *C*.
4. Look up *C* in `docs/github_repos` using the general [Verification](#verification) steps (SHA-1, prefix size 3).

If *C* is present and the `.ots` proof verifies, that commit—and therefore that snapshot of the project’s default-branch history—existed by the attested time.

