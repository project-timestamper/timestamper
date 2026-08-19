# Timestamper collections

Project description: [projecttimestamper.org](https://projecttimestamper.org/).

This repo stores the timestamped hash lists. Each collection is a directory of prefix files plus matching OpenTimestamps proofs.

## Layout

Hashes are packed as raw concatenated digests (not hex text). Files are named by the uppercase hex prefix of each hash.

```
docs/<collection>/<PREFIX>      # binary hash list
docs/<collection>/<PREFIX>.ots  # OpenTimestamps proof of SHA-256(prefix file)
```

Hosted copies: `https://arthuredelstein.github.io/timestamper/<collection>/<PREFIX>`

## Verify

The `.ots` file timestamps the whole prefix file, not each individual hash.

1. Hash the work with that collection’s **hash type** (file digest, torrent infohash, or Sci-Hub MD5).
2. Take the first **prefix size** hex digits of the digest, uppercase, and load `docs/<collection>/<PREFIX>` (or the hosted URL).
3. Confirm the digest appears in that file as raw bytes (length = **bytes/hash**).
4. SHA-256 the prefix file and check that `<PREFIX>.ots` is an OpenTimestamps proof of that SHA-256.
5. Verify the `.ots` proof against Bitcoin (for example `ots verify`). Success means that prefix list, and therefore this hash and the work, existed by the attested block time.


## Collections

| Collection | Size | Hash type | Bytes/hash | Prefix size | Timestamped |
|---|---|---|---|---|---|
| gutenberg_books | ~72K | SHA-256 | 32 | 2 | 2024-09-19 |
| libgen_fiction | ~3.03M | SHA-256 | 32 | 3 | 2024-09-16 |
| libgen_nonfiction | ~4.37M | SHA-256 | 32 | 3 | 2024-09-16 |
| scihub_articles | ~85.1M | MD5 | 16 | 4 | 2024-10-11 |
| tpb_movies | ~822K | SHA-1 | 20 | 3 | 2024-09-19 |
| wikiart_works | ~192K | SHA-256 | 32 | 2 | 2025-02-27 |
| yts_movies | ~135K | SHA-1 | 20 | 3 | 2024-09-19 |
| annas_music | ~86M | SHA-256 | 32 | 4 | 2025-12-27 |
| annas_music_with_embedded_meta | ~85M | SHA-256 | 32 | 4 | 2025-12-27 |

Hash type often use whatever the source catalog used: file SHA-256 for most collections, torrent infohashes (SHA-1) for `tpb_movies` and `yts_movies`, Sci-Hub MD5s for `scihub_articles`. `annas_music` hashes the original audio; `annas_music_with_embedded_meta` hashes the copy with embedded tags.

Prefix size is hex digits (`00` = 2, `000` = 3, `0000` = 4), so 256, 4096, or 65536 prefix files.

Timestamped dates are Bitcoin block times from the `.ots` attestations.
