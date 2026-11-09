# ChunkSwarm benchmark results

- Machine: Windows 11, 12 CPU threads, Python 3.14.3
- Date: 2026-11-09 16:19
- Runs per configuration: 3 (median shown, min-max in brackets)
- All transfers over real TCP on 127.0.0.1, every chunk SHA-256 verified

## Parallel peers (each limited to 5 MB/s)

| Peers | MB/s | Seconds |
| --- | ---: | ---: |
| 1 | 4.7 (4.7-4.8) | 13.27 (13.24-13.32) |
| 2 | 9.2 (9.1-9.2) | 6.87 (6.87-6.90) |
| 3 | 13.3 (13.1-13.4) | 4.73 (4.70-4.82) |
| 4 | 17.0 (16.2-17.1) | 3.70 (3.68-3.89) |

## Chunk size (1 peer, no limit)

| Chunk (KiB) | MB/s | Seconds |
| --- | ---: | ---: |
| 256 | 44.0 (40.0-49.9) | 4.77 (4.20-5.24) |
| 1024 | 68.6 (55.7-70.6) | 3.06 (2.97-3.77) |
| 4096 | 39.4 (38.7-46.5) | 5.32 (4.51-5.42) |

## File size (1 peer, no limit)

| File (MB) | MB/s | Seconds |
| --- | ---: | ---: |
| 10 | 48.4 (38.8-48.6) | 0.22 (0.22-0.27) |
| 50 | 81.0 (78.8-81.5) | 0.65 (0.64-0.67) |
| 100 | 70.3 (56.4-72.4) | 1.49 (1.45-1.86) |
| 200 | 73.4 (72.3-76.6) | 2.86 (2.74-2.90) |

## Failover (3 limited peers)

| Case | MB/s | Seconds |
| --- | ---: | ---: |
| no failure | 13.3 (13.2-13.6) | 4.72 (4.63-4.77) |
| one peer dies | 11.0 (7.7-11.1) | 5.74 (5.68-8.18) |

Speed-up with 4 peers vs 1: **3.59x**
