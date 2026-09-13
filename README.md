# Project Timestamper

## Overview

This repository contains the primary data files for Project Timestamper, an effort to preserve the integrity of humanity's cultural and scientific achievements.

The data consists of verifiable timestamp proofs for a number of collections of human works, including books, research papers, movies, paintings, and music. The proofs are arranged in small, flat, static files, in order to ensure that the verification process is quick, low bandwidth, and future-proof.

This repository includes no copyrighted content, files, or metadata: only cryptographic digests. A digest cannot be used to search for or reconstruct an original work; it can be used only to verify that a work you already possess existed on a certain date.

For more information on Project Timestamper, please see https://projecttimestamper.org.

## Description of collections

Each collection's set of proofs is in its own directory. These proofs are static files containing hash list files and an OpenTimestamps (`.ots`) attestation of those files.

| Collection | Item count | Hash algorithm | Bytes/hash | Prefix size (hex digits) | Timestamp date | Hash source |
|---|---|---|---|---|---|---|
| gutenberg_books | ~72K | SHA-256 | 32 | 2 | 2024-09-19 | Computed |
| libgen_fiction | ~3.03M | SHA-256 | 32 | 3 | 2024-09-16 | Source database |
| libgen_nonfiction | ~4.37M | SHA-256 | 32 | 3 | 2024-09-16 | Source database |
| scihub_articles | ~85.1M | MD5 | 16 | 4 | 2024-10-11 | Source database |
| tpb_movies | ~822K | SHA-1 | 20 | 3 | 2024-09-19 | Infohashes |
| wikiart_works | ~192K | SHA-256 | 32 | 2 | 2025-02-27 | Computed |
| yts_movies | ~135K | SHA-1 | 20 | 3 | 2024-09-19 | Infohashes |
| annas_music | ~86M | SHA-256 | 32 | 4 | 2025-12-27 | Source database |
| annas_music_with_embedded_meta | ~86M | SHA-256 | 32 | 4 | 2025-12-27 | Source database |
| ncbi_genomes | ~4.2M | SHA-256 | 32 | 3 | 2026-08-21 | Computed |
| annas_archive_torrents | ~25K | SHA-1 | 20 | 2 | 2026-09-08 | Infohashes |
| annas_literature_hashes | ~17.0M | MD5 | 16 | 4 | 2026-09-09 | Torrent metadata |
| epo_patents | ~7.01M | SHA-256 | 32 | 3 | 2026-09-09 | Computed |

Hash source indicates how the digests were obtained: 
- *Computed*: digests computed by Project Timestamper
- *Source database*: digests computed by source
- *Infohashes*: digests extracted from torrent links
- *Torrent metadata*: content digests extracted from torrent file lists

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

Automated tools for verification in https://github.com/project-timestamper/stamper.

## Common Crawl index proofs

Common Crawl’s monthly CDX index is too large to store per-capture digests in this repository. Instead, proofs target **ZipNum blocks**: each `cdx-*.gz` shard is a concatenation of gzip members (~3000 CDX lines each). Project Timestamper records the **SHA-256 of each compressed member** in a binary hash list **per shard** (not prefix-partitioned), plus a corresponding `.ots` file:

```
docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN      # ~2900 × 32-byte digests (~93 KB)
docs/common_crawl_blocks/<CRAWL>/cdx-NNNNN.ots  # OpenTimestamps attestation
```

The hash list filename is the CDX `part` with `.gz` stripped (e.g. `cdx-00066.gz` → `cdx-00066`). Digests are concatenated in block order within that shard.

| Scope | ≈ ZipNum blocks | Hash list size (32 bytes/hash) |
|---|---:|---:|
| One month (~300 shards) | ~873K | ~28 MB |
| All crawls in `collinfo.json` (~127) | ~111M | ~3.5 GB |

The CDX shards themselves stay on Common Crawl (`data.commoncrawl.org`). Block location at verify time comes from Common Crawl’s CDX API (`showPagedIndex`), so no large SURT→block locator need be hosted here.

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
   (or the hosted copies under `https://project-timestamper.github.io/timestamper/common_crawl_blocks/<CRAWL>/…`).
   Confirm `H` is present as raw 32-byte digests in that file, then verify the `.ots` proof (for example `ots verify cdx-00066.ots`).
6. Gunzip the block and confirm it contains the expected CDX line (URL / digest).
7. Use the line’s WARC `filename` / `offset` / `length` for a second range request to `data.commoncrawl.org`, download the payload, hash it with SHA-1 and verify a match with the CDX `digest`.

Typical verify traffic is a few small requests on the order of **~0.4 MB** (hash list ~93 KB + block ~280 KB, excluding WARC payload).


