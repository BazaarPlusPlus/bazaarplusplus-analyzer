# Analyzer operational measurements

Measurements were taken on Apple Silicon against the production Bundle Server
using read-only collection and local fact writes. No consumer object was
published during measurement.

## Bundle retention behavior

The server returns HTTP 410 with `window_expired` for hours outside retention
and HTTP 200 with an empty `items` array for a retained hour with no Bundles.
The source adapter preserves this distinction; an expired day is abandoned,
while a retained zero-Bundle hour can be committed.

## Collection throughput

A busy Source Hour contains roughly 2,400 Bundles. Initial conservative
collection took five to six minutes per busy hour. Because the workload is
latency-bound, increasing download concurrency from 4 to 8 approximately
halved collection time. Concurrency 64 produced no observed server-side
pushback when the HTTP keepalive pool was sized to match.

## Local fact storage

Busy hours produce roughly 17–22 MiB of Parquet, dominated by Battle Cards.
Five sealed Source Days occupied about 1.9 GiB. Facts are intentionally retained
as local recovery state; offsite backup policy remains an operator decision.

## Analysis resources

Analysis of five Source Days containing 2.79 million Battles took about four
seconds and stayed below 1 GiB RSS after build aggregation was bounded. The
seven-day offline acceptance fixture remains below the 8 GiB build limit.

Projection flushes fixed 50,000-row Arrow batches. A 2,500-Bundle synthetic
hour with 1.13 million Battle Card rows peaked 159 MiB above its fresh-process
baseline and produced 23 Parquet row groups. A production busy-hour heal with
734,544 rows peaked at 0.65 GiB for the whole process, below the 1 GiB hourly
ingest limit.

## Consumer snapshot size

The prior five-day measurement produced about 1 MiB across all legacy files.
The current architecture removes manifest, quality, daily-object, and history
overhead and publishes only the two latest snapshots, each spanning up to
seven days. Exact sizes should be remeasured after the first coordinated
production publication.
