# Timestamper collections

| Collection | Size | Hash type | Prefix size | Timestamped |
|---|---|---|---|---|
| gutenberg_books | ~72K | SHA-256 | 2 | 2024-09-19 |
| libgen_fiction | ~3.03M | SHA-256 | 3 | 2024-09-16 |
| libgen_nonfiction | ~4.37M | SHA-256 | 3 | 2024-09-16 |
| scihub_articles | ~85.1M | MD5 | 4 | 2024-10-11 |
| tpb_movies | ~822K | SHA-1 | 3 | 2024-09-19 |
| wikiart_works | ~192K | SHA-256 | 2 | 2025-02-27 |
| yts_movies | ~135K | SHA-1 | 3 | 2024-09-19 |
| annas_music | ~86M | SHA-256 | 4 | 2025-12-27 |
| annas_music_with_embedded_meta | ~85M | SHA-256 | 4 | 2025-12-27 |

Prefix size is the number of hex digits used to split files (`00` = 2, `000` = 3, `0000` = 4).

Timestamped dates are the Bitcoin block times from the OpenTimestamps attestations.
