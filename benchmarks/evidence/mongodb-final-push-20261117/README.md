# MongoDB performance push: final measurement

Before/after evidence for the MongoDB work
(key blocks, `find` instead of
`aggregate`, raw-BSON row decoding, the opt-in native `OP_MSG` read and
write path). All numbers come from one 4-vCPU Linux host
(Intel Xeon @ 2.10 GHz, kernel 6.18) shared by `mongod`, the application
and the load generator; they are not comparable to the Darwin arm64 rows
elsewhere in `docs/BENCHMARKS.md`.

## Variants

| Name | Source | Binary | Data path |
|---|---|---|---|
| BASE | `c172550` (Mongo bench added, library identical to `72ca0a2`) | `siderite_todo` sha256 `2002843a…b30d` | driver |
| FINAL-DRIVER | `1e4d4b6` (HEAD), `--features mongodb-native` | `siderite_todo` sha256 `9997e489…8ffc` | `SIDERITE_MONGO_DRIVER=driver` |
| FINAL-NATIVE | same binary as FINAL-DRIVER | same | `SIDERITE_MONGO_DRIVER=native` |

BASE was built from a separate `git worktree` with its own
`CARGO_TARGET_DIR` (a shared target directory had earlier mixed the two
trees' artifacts). Each binary was copied out and checked with the
`mongod` profiler (`e2e/probe.py.txt`, output in
`e2e/binary-verification.txt`): BASE sends `findAndModify` before every
insert and `aggregate` for `GET /todos/1`; FINAL-DRIVER sends one
`findAndModify` then plain `insert`s and a `find`, all with
`$clusterTime`; FINAL-NATIVE sends the same commands without
`$clusterTime` (the native path does not gossip cluster time). The runner
manifests record the HEAD binary's sha256 (`9997e489…`).

## Setup

- MongoDB 8.0.15, single-node replica set `rs0` on 127.0.0.1:27017,
  `--wiredTigerCacheSizeGB 2`, default write concern `majority`
  (journaled). Pool 10 (`maxPoolSize=10`, `DATABASE_POOL_SIZE=10`).
- rustc/cargo 1.99.1, release profile (fat LTO, one codegen unit),
  `mongodb` crate 3.9.1.
- FastAPI baseline: Python 3.13.18, FastAPI 0.163.1, Uvicorn 0.54.0 with
  uvloop 0.23.0 and httptools 0.9.0, Motor 3.7.1 / PyMongo 4.20.0, one
  Uvicorn worker. FastAPI reserves todo keys in blocks of 64 like
  Siderite (`benchmarks/fastapi/mongo_keys.py`).
- ApacheBench 2.3.

## What was run

1. **End-to-end, interleaved** (`e2e/`): `ab -k`, `GET /todos/1` and
   `GET /todos` (20 rows) at c=50, `POST /todos` at c=20,
   `TOKIO_WORKER_THREADS=1` and `4`, five rounds of 10 s per
   variant/endpoint/worker count, the order rotated and reversed each
   round, fixture reseeded before list and insert runs. Server, `mongod`
   and `ab` CPU from `/proc` and `rusage`. 90 runs, 0 failed, 0 non-2xx.
   Script: `e2e/e2e.py.txt` (`VARIANTS` names binary, URL suffix
   `&maxPoolSize=10` and `SIDERITE_MONGO_DRIVER`). Results:
   `e2e/results.json`, table `e2e/table.md`.
2. **Criterion** `crates/siderite-bench/benches/mongo.rs`
   (`MONGODB_URL=mongodb://127.0.0.1:27017/?replicaSet=rs0 cargo +1.99
   bench -p siderite-bench --bench mongo`), HEAD and BASE three times
   each, alternating (BASE from its worktree and target directory).
   Point estimates in `criterion/*.txt`, medians in
   `criterion/summary.txt`.
3. **Runner versus FastAPI** (`driver/`, `native/`):
   `MONGODB_URL='mongodb://127.0.0.1:27017/siderite?replicaSet=rs0'
   python benchmarks/run_benchmarks.py --suite db --db mongodb
   --mongo-driver driver --timeout 400`. Default trial counts (five
   alternating pairs, at least 10 s per timed trial, no keep-alive),
   one Rust scheduler worker and one Uvicorn worker (manifests record the
   observed worker counts). `--timeout 400` because FastAPI at about
   1,100 req/s needs more than the default 120 s for the calibrated
   request counts. The native profile ran as two invocations:
   `--todo-operation reads --requests 260000` (a cold one-second pilot
   calibrated `get one` to 195,487 requests, which the warmed native app
   served in under 10 s, so the trial was rejected) and
   `--todo-operation insert` with the defaults.

## Harness findings fixed on the way

- **Reseed reissued reserved keys.** The runner reseeds between warm-up
  and timed run inside one application process. The fixture reset the
  key counter to 100 while each app still held the unused rest of a
  64-key block, so the counter handed those keys out again: every insert
  trial failed with exactly 11 duplicate-key responses (501 warm-up and
  preflight inserts = 7 × 64 + 53). The 2026-10-03 evidence predates key
  blocks. `database_fixtures.py` now only raises the counter to 100 on a
  reseed and resets it when the fixture is recreated
  (`test_mongo_fixture.py`).
- **FastAPI without uvloop/httptools.** A first complete driver run used
  a virtualenv with plain `uvicorn` (asyncio + h11) instead of
  `uvicorn[standard]` from `fastapi/requirements.txt`. With the extras
  installed FastAPI rose by about 10-15 % (list 1,019 → 1,178 req/s,
  get one 1,100 → 1,228, insert 990 → 1,125); only that run is
  reported. The run manifest now records `uvloop` and `httptools`.

`report.md` holds the runner's three reports. The driver run recorded
source `1e4d4b6` (dirty: the reseed fix, committed seconds later as
`51b3b72`); the native runs recorded `51b3b72` (dirty: manifest and
changelog edits). The fixture code was identical in all three.

## Results

### 1. End-to-end, BASE vs FINAL (interleaved, `ab -k`)

Medians of five 10 s rounds (min-max in brackets); CPU columns are CPU
time per request of each process.

| Endpoint | Workers | Variant | Median req/s (min-max) | vs BASE | p50 / p99 ms | Server CPU µs/req | mongod CPU µs/req | ab CPU µs/req | Machine busy |
|---|---|---|---|---|---|---|---|---|---|
| GET /todos/1 | 1 | BASE | 16,270 (15,767-17,355) | 1.00x | 2.95 / 5.80 | 60.5 | 104.3 | 15.1 | 74% |
| GET /todos/1 | 1 | FINAL-DRIVER | 16,899 (16,179-18,249) | 1.04x | 2.85 / 5.37 | 58.3 | 73.4 | 12.4 | 63% |
| GET /todos/1 | 1 | FINAL-NATIVE | 28,250 (27,823-29,257) | 1.74x | 1.75 / 3.31 | 34.8 | 52.8 | 13.0 | 73% |
| GET /todos | 1 | BASE | 11,477 (11,307-12,103) | 1.00x | 4.24 / 7.24 | 85.8 | 172.8 | 18.3 | 81% |
| GET /todos | 1 | FINAL-DRIVER | 14,447 (14,001-14,658) | 1.26x | 3.34 / 6.05 | 68.4 | 123.1 | 16.8 | 77% |
| GET /todos | 1 | FINAL-NATIVE | 21,696 (20,788-22,890) | 1.89x | 2.19 / 4.12 | 44.0 | 95.7 | 18.1 | 88% |
| POST /todos | 1 | BASE | 2,374 (2,199-2,385) | 1.00x | 7.94 / 17.72 | 183.2 | 924.7 | 26.2 | 71% |
| POST /todos | 1 | FINAL-DRIVER | 5,497 (5,230-5,608) | 2.31x | 3.41 / 7.90 | 94.8 | 332.1 | 18.7 | 65% |
| POST /todos | 1 | FINAL-NATIVE | 6,389 (6,135-6,514) | 2.69x | 2.90 / 7.16 | 59.0 | 313.5 | 19.2 | 67% |
| GET /todos/1 | 4 | BASE | 13,068 (12,416-13,371) | 1.00x | 3.63 / 7.01 | 126.2 | 123.4 | 14.2 | 86% |
| GET /todos/1 | 4 | FINAL-DRIVER | 15,770 (14,615-16,014) | 1.21x | 3.07 / 5.57 | 111.3 | 87.3 | 12.3 | 84% |
| GET /todos/1 | 4 | FINAL-NATIVE | 30,870 (30,822-32,849) | 2.36x | 1.55 / 3.22 | 43.1 | 56.6 | 9.9 | 85% |
| GET /todos | 4 | BASE | 8,783 (8,169-8,969) | 1.00x | 5.59 / 10.26 | 192.3 | 203.4 | 18.7 | 92% |
| GET /todos | 4 | FINAL-DRIVER | 11,779 (11,533-12,656) | 1.34x | 4.07 / 7.68 | 136.1 | 142.0 | 15.8 | 87% |
| GET /todos | 4 | FINAL-NATIVE | 19,636 (18,072-20,084) | 2.24x | 2.44 / 4.90 | 61.9 | 107.7 | 15.1 | 91% |
| POST /todos | 4 | BASE | 2,124 (1,695-2,160) | 1.00x | 8.86 / 20.41 | 323.9 | 971.6 | 25.1 | 74% |
| POST /todos | 4 | FINAL-DRIVER | 4,998 (4,660-5,256) | 2.35x | 3.78 / 8.80 | 160.4 | 351.1 | 18.0 | 70% |
| POST /todos | 4 | FINAL-NATIVE | 5,872 (5,286-6,003) | 2.76x | 3.18 / 7.68 | 96.0 | 330.2 | 15.7 | 69% |

User/system split at four workers (`e2e/cpu-split.txt`; one extra 10 s
run per case, so indicative only), µs per request:

| Case | Variant | Server usr / sys | mongod usr / sys |
|---|---|---|---|
| GET /todos/1 | BASE | 79.7 / 31.1 | 85.7 / 19.6 |
| GET /todos/1 | FINAL-DRIVER | 63.9 / 28.7 | 52.7 / 16.3 |
| GET /todos/1 | FINAL-NATIVE | 20.2 / 17.8 | 37.1 / 14.5 |
| GET /todos | BASE | 132.7 / 44.5 | 143.4 / 24.9 |
| GET /todos | FINAL-DRIVER | 93.3 / 35.4 | 109.2 / 20.5 |
| GET /todos | FINAL-NATIVE | 33.9 / 21.9 | 79.2 / 20.2 |
| POST /todos | BASE | 200.1 / 84.5 | 609.6 / 202.9 |
| POST /todos | FINAL-DRIVER | 94.1 / 43.7 | 205.5 / 84.4 |
| POST /todos | FINAL-NATIVE | 50.6 / 34.2 | 189.4 / 84.8 |

### 2. Criterion `mongo` bench (µs, median of three alternating runs)

| Operation | BASE | HEAD | HEAD/BASE |
|---|---|---|---|
| get_pk: ORM (driver) | 140.8 | 103.6 | 0.74 |
| get_pk: ORM native | – | 78.9 | |
| get_pk: driver `find_one` | 93.7 | 103.6 | 1.11 (unchanged code; run spread 93-105) |
| get_pk: driver `aggregate` pipeline | 132.8 | 137.3 | 1.03 |
| get_pk: hand-written `OP_MSG` find (floor) | 69.6 | 68.8 | 0.99 |
| list 20: ORM (driver) | – | 185.2 | |
| list 20: ORM native | – | 135.0 | |
| list 20: driver raw | – | 160.4 | |
| insert w:1: ORM (driver) | 524.6 | 161.2 | 0.31 |
| insert w:1: ORM native | – | 126.7 | |
| insert w:1: driver counter + insert | 521.1 | 540.4 | 1.04 |
| insert w:1: driver insert, client key | 141.7 | 146.7 | 1.04 |
| insert w:1: `OP_MSG` insert (floor) | 80.9 | 82.7 | 1.02 |
| insert majority: ORM (driver) | 2,226 | 855 | 0.38 |
| insert majority: ORM native | – | 787 | |
| insert majority: driver insert, client key | 891 | 909 | 1.02 |
| insert majority: `OP_MSG` insert (floor) | 634 | 629 | 0.99 |

BASE has no `mongo_list` group and no native variants.

### 3. Runner versus FastAPI (one worker each, no keep-alive)

| Test | Median req/s (S / F) | Paired ratio (95% CI) | p99 ms (S / F) | Adoption / Strong lead |
|---|---|---|---|---|
| `mongodb: list 20` | 11,521 / 1,178 | 9.833x (9.559-10.095) | 7.827 / 56.351 | pass / pass |
| `mongodb: get one` | 13,266 / 1,228 | 10.904x (10.747-11.113) | 6.686 / 55.065 | pass / pass |
| `mongodb: insert` | 6,125 / 1,125 | 5.419x (5.282-5.504) | 7.388 / 29.883 | pass / pass |
| `mongodb-siderite-native: list 20` | 14,923 / 1,008 | 14.906x (14.798-15.015) | 6.874 / 64.297 | pass / pass |
| `mongodb-siderite-native: get one` | 19,185 / 1,062 | 17.866x (17.373-18.387) | 5.487 / 60.781 | pass / pass |
| `mongodb-siderite-native: insert` | 6,390 / 1,075 | 5.930x (5.844-5.991) | 7.791 / 31.057 | pass / pass |

Five identified pairs per row; every timed trial lasted at least 13.4 s.
FastAPI's single Python process is CPU-bound at about 1,000-1,250 req/s
on this host for all three operations, so the ratios mostly reflect
FastAPI's per-request cost here and are much larger than the 2026-10-03
ratios taken on a faster host.

## Analysis

- **Reads, driver path.** `find` instead of `aggregate` and raw-BSON
  decoding cut ORM `get` from 141 to 104 µs (now beside the driver's own
  `find_one`) and `mongod` CPU per `GET /todos/1` from 104 to 73 µs.
  End to end that is 1.04x at one worker, where the server is CPU-bound
  and its CPU per request barely moved, and 1.21x / 1.34x at four
  workers, where `mongod` shares the four CPUs.
- **Reads, native path.** The `OP_MSG` path takes ORM `get` to 79 µs,
  about 10 µs above the hand-written floor, and cuts server CPU per
  request again (58 → 35 µs at one worker, 111 → 43 µs at four).
- **Inserts.** Key blocks remove the `findAndModify` before each insert:
  ORM `w:1` insert 525 → 161 µs (the bare driver needs 147 µs with a
  client key), `mongod` CPU per `POST` 925-972 → 313-351 µs, and
  `POST /todos` at 2.3x BASE on the driver path and 2.7-2.8x with native
  writes.
- **Where the time goes now (measured).** With native reads at four
  workers the machine is saturated (85-91 % busy) and `mongod` spends
  more CPU per request than Siderite (57 vs 43 µs for `GET /todos/1`,
  108 vs 62 µs for `GET /todos`), 52-58 % of all CPU per request. About
  half of Siderite's CPU per `GET /todos/1` is system time (two sockets
  per request). Inserts are bound by `mongod`: 75-80 % of the CPU per
  request plus majority/journal commit waits, with the machine 67-69 %
  busy.
- **Inferred, not measured.** How the remaining ~15-20 µs of user-space
  client CPU per `GET` divides between HTTP handling and ORM/`OP_MSG`
  work; no profile was taken in this run.
