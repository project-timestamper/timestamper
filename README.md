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

Hash source indicates how the digests were obtained: 
- *Computed*: digests computed by Project Timestamper
- *Source database*: digests computed by source
- *Infohashes*: digests extracted from torrent links

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

Automated tools for verification are planned.


