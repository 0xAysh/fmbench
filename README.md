# fmbench

The bench kit for FilterMaps in Reth: every number the gates G1, G4, G7 and G8 need, measured
on the node's own machine. Python 3.10 or newer, standard library only. The spec is
`../docs/specs/bench-kit.md`; the gates are in `gates.json`.

```sh
python3 -m unittest            # the tests
python3 -m fmbench <command> -h
```

## The node

- Run the kit as the node's user on the node's machine: it reads `/proc/<pid>` of the `reth`
  systemd unit (`--unit`) for the binary, its commit and its flags.
- `--mode` must match the node: `index` needs `--filter-maps`, `bloom` and `today` need it off.
  A run with the wrong mode is refused.
- Flags (roadmap §1.4): `--rpc.max-blocks-per-filter 0 --rpc.max-logs-per-response 0
  --rpc.max-response-size 1024`, `--metrics 127.0.0.1:9001`, and for `watch`
  `--log.file.format json` (block times come from the JSON file log).
- `cold` and `run --cache cold` stop the unit, drop the page cache and start it, through
  `sudo -n` when not root, then wait until the node is back at the chain tip (latest block at
  most 24 s old) and it has persisted blocks since the start
  (`reth_consensus_engine_beacon_persistence_duration_count`: the catch-up is written in one
  batch, which a query must not overlap). In index mode it also waits until the indexer has
  finished a step since the start (`reth_filter_maps_step_duration_seconds_count`), with no failed
  step (`reth_filter_maps_failures`) and its head back at its value before the stop. That first
  step renders the map group in progress again (up to 1,000 blocks per step); later steps render
  only newly finalized blocks, as in a warm run, so `cold` does not wait for finality. A failed
  step stops `cold` with an error.

## Commands

| Command | Writes | For |
|---|---|---|
| `sample` | `queries/{g7,g1,oneshot}.jsonl` | the query files, once per baseline (index mode) |
| `run` | `seq.jsonl` | G7 (`g7.jsonl`), G1 (`g1.jsonl`), D2 (`oneshot.jsonl`) |
| `equal A B` | exit 1 on any difference | G1 |
| `load` | `load.jsonl` | G8 |
| `sweep` | `sweep.jsonl` | the backfill number, over the query file's window by default |
| `watch` | `watch.csv`, `blocks.csv` | build rate, G3 lag, G4 |
| `cold` | — | `--dry-run` prints the steps |
| `report DIR` | `report.md` and the chart CSVs | every gate |

Each result goes to `results/<commit>/<run-id>/` with a `meta.json` (binary, commit, flags,
mode, cache state, kernel, filesystem, CPU, RAM, query file hash). `report` reads one
`results/<commit>/` and uses the latest finished run of each kind; the G7, G1 and one-shot
tables compare the latest index-mode run with the bloom and today runs of the same query file,
`--limit` and `--reps`, so a newer smoke run is never compared with the matrix; its heading names
the judged run. A G7 run with fewer than 5 repetitions (roadmap §1.4) gets no verdict. D2 uses
the warm G7 bloom run only when its query file comes from the one-shot's sample (same window and
seed) and it has 5 repetitions or more.
The Reth 2.7.0 runs (mode
`today`) land under that binary's commit: pass `--today results/<2.7.0 commit>/` to fill the
today columns.

## Box gates (bench-kit spec, B2 step 0)

```sh
# 2. index mode: query files, then 20 g7 queries
python3 -m fmbench sample
python3 -m fmbench run --mode index --queries queries/g7.jsonl --limit 20 --reps 1
# 3. G1 smoke: 100 g1 queries per mode (restart the node without --filter-maps between)
python3 -m fmbench run --mode index --queries queries/g1.jsonl --limit 100 --reps 1
python3 -m fmbench run --mode bloom --queries queries/g1.jsonl --limit 100 --reps 1
python3 -m fmbench equal results/<commit>/run-index-warm-g1-*/seq.jsonl results/<commit>/run-bloom-warm-g1-*/seq.jsonl
# 4. load, 8 clients, 30 s
python3 -m fmbench load --mode index --queries queries/g7.jsonl --clients 8 --duration 30
# 5. one Q1 sweep at chunk 100,000, per mode
python3 -m fmbench sweep --mode index --queries queries/g7.jsonl --query Q1-w100000-000 --chunk 100000
# 6. watch for 10 minutes
python3 -m fmbench watch --mode index --phase catchup --log-file /data/logs/mainnet/reth.log --duration 600
# 7. report
python3 -m fmbench report results/<commit>/
```

The G7 matrix: `run --queries queries/g7.jsonl` in each mode, `--cache warm --reps 25` and
`--cache cold --reps 5`, plus one `--spread` repeat per mode and cache state for the run-to-run
spread (same file, `--limit` and `--reps` as the run it repeats). The one-shot:
`run --queries queries/oneshot.jsonl --reps 1` per mode, before the matrix.

G4: `watch --phase off` (bloom mode) is the catch-up's reference, and `watch --phase load` runs
during the G8 load in both modes: the bloom-mode one is the reference of the index-mode one.

`sweep` covers the query file's window by default, not the whole indexed window: the window
starts `--tail-margin` blocks above the first indexed block, so both modes sweep the same range
while the index tail moves, and ends 2,000 blocks below the index head. A restarted index node
reads the receipts of its newest map group again, a bloom-mode node does not, so a cold query
there would favour the index. Q6 and Q6d (the rare and the popular filters, judged by the
selective and the dense rule) and Q7 (the wildcard, which stays under the result cap only over a
few blocks) run at 1, 10 and 100 blocks; the header of each query file lists the queries
the cap dropped (`dropped_by_cap`).
