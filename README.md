# EventMeter

EventMeter gives Ruby applications a small event-based runtime metrics layer.

It records application events, processes them into storage-backed rollups, and
answers operational questions:

- How many times did this run?
- How fast is it running?
- How long did each run take?
- How often does the same thing run again?
- What changed between two time windows?

EventMeter does not send data to an external service. Your app chooses the
storage, and the metrics stay inside your infrastructure.

## Install

```ruby
gem "event_meter"
```

```ruby
require "event_meter"
```

EventMeter does not force Redis or PostgreSQL into every install. Add the store
client your app chooses:

```ruby
gem "redis" # for Redis stream or rollup storage
gem "pg"    # for PostgreSQL rollup storage
```

### Rails With File Stream And PostgreSQL Rollups

For Rails apps, the lowest-friction production setup is usually:

- file stream storage for fast local event writes
- PostgreSQL rollup storage for durable, shared reports

Add the gem:

```ruby
gem "event_meter"
```

Generate the migration and initializer:

```sh
bin/rails generate event_meter:install \
  --namespace billing_app:event_meter:v1 \
  --table-prefix event_meter

bin/rails db:migrate
```

The generator creates three PostgreSQL tables:

- `event_meter_rollups`
- `event_meter_strings`
- `event_meter_processed_entries`

It also creates `config/initializers/event_meter.rb`:

```ruby
require "event_meter/rails"

EventMeter::Rails.configure(
  namespace: "billing_app:event_meter:v1",
  stream_storage: :file,
  stream_path: Rails.root.join("tmp/event_meter/billing_app-event_meter-v1").to_s,
  rollup_storage: :postgres,
  table_prefix: "event_meter",
  stream_sync: :flush,
  auto_cleanup_history: true,
  logger: Rails.logger
)
```

If you prefer to keep settings in `config/application.rb` or environment files:

```ruby
# config/application.rb
config.event_meter = ActiveSupport::OrderedOptions.new
config.event_meter.namespace = "billing_app:event_meter:v1"
config.event_meter.stream_storage = :file
config.event_meter.stream_path = Rails.root.join("tmp/event_meter/billing_app-event_meter-v1").to_s
config.event_meter.rollup_storage = :postgres
config.event_meter.table_prefix = "event_meter"
config.event_meter.stream_sync = :flush
config.event_meter.auto_cleanup_history = true
config.event_meter.cleanup_history_retention = 31.days.to_i
config.event_meter.cleanup_history_interval = 1.hour.to_i
config.event_meter.summary_key_limit = 10_000
```

```ruby
# config/initializers/event_meter.rb
require "event_meter/rails"

settings = Rails.application.config.event_meter

EventMeter::Rails.configure(
  namespace: settings.namespace,
  stream_storage: settings.stream_storage,
  stream_path: settings.stream_path,
  rollup_storage: settings.rollup_storage,
  table_prefix: settings.table_prefix,
  stream_sync: settings.stream_sync,
  auto_cleanup_history: settings.auto_cleanup_history,
  cleanup_history_retention: settings.cleanup_history_retention,
  cleanup_history_interval: settings.cleanup_history_interval,
  summary_key_limit: settings.summary_key_limit,
  logger: Rails.logger
)
```

After boot, every Rails process shares the same EventMeter configuration.
Instrumentation can call `EventMeter.start(...)`, scheduled jobs can call
`EventMeter.process_pending(...)`, and consoles can call `EventMeter.summary(...)`
directly.

## Quick Start

Configure storage:

```ruby
EventMeter.configure do |config|
  config.namespace = "billing_app:event_meter:v1"
  config.redis = -> { Redis.new }
  config.auto_cleanup_history = true
end
```

Record one unit of work:

```ruby
event = EventMeter.start("invoice_delivery",
  customer_id: 42,
  provider: "postmark",
  queue: "mailers",
  worker_id: "mail-1"
)

deliver_invoice
result = event.success(message_id: "msg_123")

warn "EventMeter write failed: #{result.error.message}" if result.error?
```

Process pending events into a versioned report:

```ruby
EventMeter.process_pending("invoice_delivery", version: 1) do |report|
  report.index_by(:customer_id)
  report.index_by(:provider)
  report.index_by(:provider, :queue)
end
```

When `auto_cleanup_history` is enabled, `process_pending` also cleans old
rollups, interval state, and leftover processed-entry markers on a
storage-backed interval. Reads such as `summary` and `series` stay read-only.

Read the report:

```ruby
EventMeter.summary("invoice_delivery",
  version: 1,
  from: Time.now.utc - 3600,
  to: Time.now.utc,
  by: { provider: "postmark" }
)
```

Example output:

```ruby
{
  count: 1260,
  success_count: 1254,
  failure_count: 6,
  skipped_count: 0,
  started_at_min: "2026-05-06T10:00:02.000000Z",
  started_at_max: "2026-05-06T10:59:58.000000Z",
  rate_window_seconds: 3600.0,
  per_second: 0.35,
  per_minute: 21.0,
  duration_ms_count: 1260,
  duration_ms_sum: 970_200,
  duration_ms_avg: 770.0,
  duration_ms_min: 42,
  duration_ms_max: 8_910,
  interval_ms_count: 0,
  interval_ms_sum: 0
}
```

## The Model

EventMeter has two phases.

Recording writes raw events:

```ruby
EventMeter.start("invoice_delivery", provider: "postmark").success
```

Processing defines the report shape and moves pending events into rollups:

```ruby
EventMeter.process_pending("invoice_delivery", version: 1) do |report|
  report.index_by(:provider)
end
```

Reports read processed rollups only:

```ruby
EventMeter.summary("invoice_delivery", version: 1, by: { provider: "postmark" })
```

The event name passed to `EventMeter.start` and `EventMeter.process_pending`
must match. If you record `"invoice_delivery"`, process `"invoice_delivery"`.

### Params

Everything your app knows about the event is a param:

```ruby
EventMeter.start("feed_refresh",
  feed_id: 77,
  provider: "shopify",
  queue: "feeds"
)
```

EventMeter does not know what a feed, customer, queue, or worker is. They are
just params that can be indexed when you need reports by them.

### Report Versions

The report version is part of the processed data shape:

```ruby
EventMeter.process_pending("feed_refresh", version: 1) do |report|
  report.index_by(:provider)
end
```

EventMeter stores the report definition metadata when processing starts. If you
change the indexes or interval rules for the same event and version, processing
raises `EventMeter::DefinitionChangedError`. Bump the version when you change
the report shape:

```ruby
EventMeter.process_pending("feed_refresh", version: 2) do |report|
  report.index_by(:provider)
  report.index_by(:provider, :queue)
end
```

Reports also take the version:

```ruby
EventMeter.summary("feed_refresh", version: 2, by: { provider: "shopify" })
```

### Indexes

An index says: "I want to query reports by this exact shape."

```ruby
EventMeter.process_pending("invoice_delivery", version: 1) do |report|
  report.index_by(:provider)
  report.index_by(:provider, :queue)
end
```

This supports:

```ruby
EventMeter.summary("invoice_delivery", version: 1)
EventMeter.summary("invoice_delivery", version: 1, by: { provider: "postmark" })
EventMeter.summary("invoice_delivery", version: 1, by: { provider: "postmark", queue: "mailers" })
```

This is not supported until you add `report.index_by(:queue)`:

```ruby
EventMeter.summary("invoice_delivery", version: 1, by: { queue: "mailers" })
```

Unsupported filters raise `EventMeter::UnsupportedQueryError`. EventMeter does
not scan raw events to answer new filters later. If a question matters, index it
before you need that report.

Every report automatically has the all-events index, so `by: {}` works without
custom indexes.

Indexed `by:` values cannot be `nil`. Events with a `nil` value simply do not
write that specific indexed rollup.

### Intervals

Intervals answer: "How long since this same thing ran before?"

Use `measure_interval_by` with the param that identifies the repeated thing:

```ruby
EventMeter.process_pending("feed_refresh", version: 1) do |report|
  report.index_by(:provider)

  report.measure_interval_by(:feed_id)
  report.measure_interval_by(:feed_id, group_by: :provider)
end
```

Read this as:

```text
Use feed_id to recognize the same feed running again.
Store one interval report for all feeds.
Store another interval report grouped by provider.
```

If feed `77` starts at `10:00` and then starts again at `10:05`, EventMeter
records one interval sample:

```text
10:05 - 10:00 = 300_000 ms
```

The grouped report includes interval fields:

```ruby
EventMeter.summary("feed_refresh", version: 1, by: { provider: "shopify" })
```

```ruby
{
  count: 2,
  interval_ms_count: 1,
  interval_ms_sum: 300_000,
  interval_ms_avg: 300_000.0,
  interval_ms_min: 300_000,
  interval_ms_max: 300_000
}
```

The identity param, `feed_id` here, does not have to be indexed. It only has to
exist in event params so EventMeter can remember the previous start time for
that value.

EventMeter uses `started_at` for interval math. Late events still count in their
original time buckets. For interval state, EventMeter only moves forward; older
timestamps do not rewrite previous interval samples.

`measure_interval_by(..., group_by: ...)` automatically creates the matching
group index. You can still write `index_by` explicitly when it makes the report
definition easier to read.

## Lifecycle API

Start an event:

```ruby
event = EventMeter.start("invoice_delivery",
  customer_id: 42,
  provider: "postmark"
)
```

Finish it once:

```ruby
event.success(message_id: "msg_123")
event.skip("disabled")
event.failure(error)
```

The lifecycle API sets EventMeter-owned fields for you:

```ruby
{
  "name" => "invoice_delivery",
  "status" => "success",
  "started_at" => "2026-05-06T10:15:20.000000Z",
  "duration_ms" => 734,
  "params" => {
    "customer_id" => 42,
    "provider" => "postmark",
    "message_id" => "msg_123"
  }
}
```

`name`, `status`, `started_at`, and `duration_ms` belong to EventMeter.
Application data goes under `params`, so your app can still pass params named
`status` or `duration_ms` without colliding with EventMeter fields.

`success`, `skip`, and `failure` return `EventMeter::WriteResult`:

```ruby
result.recorded? # true when the event reached stream storage
result.error?    # true when EventMeter could not write the event
result.payload   # event payload EventMeter tried to write
result.error     # config, validation, or storage error
```

The write path is designed for production instrumentation. `start`, `success`,
`skip`, and `failure` do not raise because storage is unavailable,
misconfigured, or temporarily broken.

```ruby
event = EventMeter.start("invoice_delivery", Object.new)
warn event.error.message if event.error?

result = event.success
warn result.error.message if result.error?
```

An event object records at most once. A second finish call returns a failed
`WriteResult` with `EventMeter::AlreadyRecordedError`.

## Processing

Recording only appends to stream storage. Reports do not process pending events
for you.

Run processing from a scheduler, worker, cron, console task, or admin action:

```ruby
result = EventMeter.process_pending("invoice_delivery", version: 1) do |report|
  report.index_by(:customer_id)
  report.index_by(:provider)
  report.measure_interval_by(:customer_id, group_by: :provider)
end

result.to_h
```

Example output:

```ruby
{
  event_name: "invoice_delivery",
  version: 1,
  processed: 842,
  skipped_already_processed: 0,
  malformed: 0,
  complete: true,
  locked: false
}
```

If another processor is holding the interval lock, `locked` is `true` and the
stream rows are left in place for a later pass.

If rollups were written but stream deletion did not finish, `complete` is
`false`. The next processing pass can read those rows again, but processed-entry
guards prevent double counting. When stream deletion later succeeds, those
guards are removed too.

## Reports

All report methods require the same event name and version that you used while
processing.

### Summary

Use `summary` for headline numbers:

```ruby
EventMeter.summary("invoice_delivery",
  version: 1,
  from: Time.utc(2026, 5, 6, 10, 0),
  to: Time.utc(2026, 5, 6, 11, 0),
  by: { provider: "postmark" }
)
```

Pass both `from:` and `to:`, or neither. With a time window, EventMeter reads
minute buckets and rates use the included bucket span.

Without a time window, EventMeter reads retained hour rollups and uses the
observed event span for rate fields:

```ruby
EventMeter.summary("invoice_delivery", version: 1, by: { provider: "postmark" })
```

No-window summaries are capped by `config.summary_key_limit`, which defaults to
10,000 retained hour buckets per query. Pass `from:` and `to:` for large report
windows, or set `summary_key_limit = nil` if your storage can safely support
unbounded retained-summary reads.

### Series

Use `series` for charts:

```ruby
EventMeter.series("invoice_delivery",
  version: 1,
  from: Time.utc(2026, 5, 6, 10, 0),
  to: Time.utc(2026, 5, 6, 10, 3),
  every: :minute,
  by: { provider: "postmark" }
)
```

Example output:

```ruby
[
  {
    bucket: "2026-05-06T10:00:00Z",
    count: 21,
    success_count: 21,
    failure_count: 0,
    skipped_count: 0,
    duration_ms_avg: 720.0,
    per_minute: 21.0
  }
]
```

By default, `series` returns the last 60 minute buckets ending at the current
minute bucket:

```ruby
EventMeter.series("invoice_delivery", version: 1)
```

### Compare

Use `compare` to read the same summary for two windows:

```ruby
EventMeter.compare("invoice_delivery",
  version: 1,
  before: Time.utc(2026, 5, 6, 9, 0)...Time.utc(2026, 5, 6, 10, 0),
  after: Time.utc(2026, 5, 6, 10, 0)...Time.utc(2026, 5, 6, 11, 0),
  by: { provider: "postmark" }
)
```

### Definition Metadata

Inspect the stored report definition:

```ruby
EventMeter.report_definition("invoice_delivery", version: 1)
```

This is useful when you need to confirm which indexes and interval rules a
processed report is using.

## Storage

EventMeter separates raw pending events from processed reports:

```text
stream_storage -> pending input buffer
rollup_storage -> processed report data
```

You can use one backend for both or mix backends.

For busy applications, a good default shape is:

```text
file stream on fast local disk -> Redis or PostgreSQL rollups
```

That keeps event recording cheap and lets report storage be chosen for how you
want to query and retain data.

### Redis For Both

```ruby
EventMeter.configure do |config|
  config.namespace = "billing_app:event_meter:v1"
  config.redis = -> { Redis.new }
end
```

When `config.redis` is a factory, EventMeter creates separate Redis clients for
data commands and refreshed process locks. That keeps long processing locks from
sharing one connection with report writes.

### File Stream And Redis Rollups

```ruby
EventMeter.configure do |config|
  config.namespace = "billing_app:event_meter:v1"

  config.stream_storage = EventMeter::Stores::Stream::File.new(
    path: "/tmp/event_meter/billing-app",
    sync: :flush
  )

  config.rollup_storage = EventMeter::Stores::Rollup::Redis.new(
    redis: Redis.new,
    namespace: config.namespace
  )
end
```

### File Stream And File Rollups

```ruby
EventMeter.configure do |config|
  config.namespace = "billing_app:event_meter:v1"

  config.stream_storage = EventMeter::Stores::Stream::File.new(
    path: "/tmp/event_meter/billing-app",
    sync: :flush
  )

  config.rollup_storage = EventMeter::Stores::Rollup::File.new(
    path: "/tmp/event_meter/billing-app"
  )
end
```

File stream storage writes one folder per event name:

```text
/tmp/event_meter/billing-app/
  streams/
    invoice_delivery-f2a49ce1aca9419c/
      logs/
      processing/
      quarantine/
      claim_locks/
```

File rollup storage writes one folder per namespace, event name, and version:

```text
/tmp/event_meter/billing-app/
  rollups/
    billing_app-event_meter-v1-ff44ee2453002022/
      invoice_delivery-f2a49ce1aca9419c/
        v1/
          definition.json
          process.lock
          hashes/
            minute/
              202605061015.json
            hour/
              2026050610.json
          strings/
            shards/
              cd.json
          processed/
            202605061015-host-1234-abcd.jsonl.processed.json
```

The readable folder names include stable hashes, so unusual namespaces and
event names stay safe on filesystems while still being recognizable.

File stream storage uses `time_bucket_stream` internally. It writes JSONL files
by UTC minute and only processes inactive past-minute files. That keeps
processors away from the active append file.

Sync modes:

```ruby
sync: :none  # fastest; OS buffers decide when bytes hit disk
sync: :flush # default; flush after each event
sync: :fsync # strongest; fsync after each event
```

### Redis Stream Storage

Redis stream storage writes one Redis stream per event name:

```text
billing_app:event_meter:v1:stream:invoice_delivery
```

By default, Redis stream storage reads all available rows. If you need a cap for
a very large backlog:

```ruby
config.redis_read_limit = 10_000
```

If you instantiate Redis stores yourself, you can pass `lock_redis:` when you
want process locks to use their own connection:

```ruby
EventMeter::Stores::Stream::Redis.new(
  redis: Redis.new,
  lock_redis: Redis.new,
  namespace: "billing_app:event_meter:v1"
)

EventMeter::Stores::Rollup::Redis.new(
  redis: Redis.new,
  lock_redis: Redis.new,
  namespace: "billing_app:event_meter:v1"
)
```

### Rollup Keys

Rollup keys include namespace, event name, report version, bucket, and index.

For `invoice_delivery` version 1:

```text
billing_app:event_meter:v1:rollup:invoice_delivery:v1:minute:202605061015:all
billing_app:event_meter:v1:rollup:invoice_delivery:v1:hour:2026050610:provider=postmark
billing_app:event_meter:v1:state:invoice_delivery:v1:interval:customer_id:42
billing_app:event_meter:v1:definition:invoice_delivery:v1
```

When multiple buckets are read, EventMeter combines them by summing counts and
sums, and by taking the min/max of min/max fields. It never averages averages.

### PostgreSQL Rollups

Rails apps should normally use `EventMeter::Rails.configure`, shown in the
install section. It wraps ActiveRecord's connection pool so request and job
processes do not share one long-lived raw `PG::Connection`.

Install the tables:

```ruby
EventMeter::Stores::Rollup::Postgres.install!(connection: connection)
```

Configure:

```ruby
EventMeter.configure do |config|
  config.namespace = "billing_app:event_meter:v1"
  config.rollup_storage = EventMeter::Stores::Rollup::Postgres.new(
    connection: connection,
    lock_connection: lock_connection,
    namespace: config.namespace
  )
end
```

`connection` and `lock_connection` should respond to `exec(sql)` and
`exec_params(sql, params)`, such as `PG::Connection` objects. Use a separate
`lock_connection` so long rollup transactions do not block lock lease refreshes.
The Rails ActiveRecord adapter creates that separate lock connection for you.

Run `process_pending` outside caller-managed database transactions. Raw
PostgreSQL storage opens its own `BEGIN`/`COMMIT` around each rollup apply, and
the ActiveRecord adapter checks out one connection for that transaction so the
same connection is used until commit or rollback.

PostgreSQL storage keeps all report data in three tables:

| Table | Purpose |
| --- | --- |
| `event_meter_rollups` | Minute and hour buckets. |
| `event_meter_strings` | Report definitions, cleanup watermark, and interval state. |
| `event_meter_processed_entries` | Temporary retry guards for stream rows that were applied but not deleted yet. |

`install!` also creates prefix indexes for cleanup and report-key scans, so old
history can be removed by SQL predicates instead of loading all keys into Ruby.

CLI helpers:

```sh
event_meter postgres schema --table-prefix event_meter
event_meter postgres install --url "$DATABASE_URL" --table-prefix event_meter
```

## Idempotency And Concurrency

Processing reads stream entries, updates rollup storage, marks entry ids as
processed, then deletes the processed stream rows.

```text
record event
  -> stream storage

process pending
  -> minute rollups
  -> hour rollups
  -> interval state
  -> processed-entry guards
  -> delete processed stream rows
  -> delete processed-entry guards for those rows
```

If rollup writes succeed but stream deletion fails, the next processing pass may
read the same stream rows again. Processed-entry markers make that safe: those
ids are skipped and do not double count. Once the stream rows are deleted,
EventMeter removes the matching processed-entry markers immediately.

File rollup storage keeps those processed-entry markers beside the stream-file
lifecycle instead of inside one large state file:

```text
rollups/billing_app-event_meter-v1-ff44ee2453002022/invoice_delivery-f2a49ce1aca9419c/v1/processed/
  202605061015-host-1234-abcd.jsonl.processed.json
```

After the stream file is deleted, EventMeter deletes the matching processed
sidecar too.

Redis and PostgreSQL rollup storage keep processed-entry markers scoped by
namespace, event name, report version, and stream entry id. That means the same
stream id can appear in different event streams or report versions without
colliding. Redis stores those markers as keys; PostgreSQL stores them in
`event_meter_processed_entries`.

Split file rollups also protect partial writes. Bucket and shard files keep a
temporary `_applied` map while a stream file is still retryable:

```json
{
  "_applied": {
    "6e559ad9...": "2026-05-06T10:15:03.000000Z"
  },
  "provider=postmark": {
    "count": "12",
    "duration_ms_sum": "8400"
  }
}
```

If processing dies after writing some bucket files but before marking every
write as finished, retrying the same stream entries skips bucket files that
already have the transaction id and applies only the missing files. Once the
stream file is deleted, EventMeter removes the sidecar and those `_applied`
markers.

Plain count and duration rollup writes are merge-safe and can run concurrently
when stream storage gives each processor different rows. Interval metrics need a
processing lock because they advance "previous start time" state. Redis and
PostgreSQL use refreshed locks with TTL-backed recovery, and file rollups use a
process lock file.

Redis stream storage also uses a refreshed processing lock so two processors do
not read the same pending stream rows at the same time. File stream storage uses
`time_bucket_stream` claims instead, so processors can safely take different
inactive files in parallel.

Rollup writes are safe for multiple processors updating the same bucket:

- counts and sums are additive
- min and max fields merge as min and max
- interval state only moves forward
- late processing writes to the event's original `started_at` bucket
- split file rollups use per-batch `_applied` guards so retries do not double count partial writes

Malformed stream rows are marked processed and deleted instead of blocking the
queue. They do not contribute to rollups.

## Cleanup

Enable automatic cleanup if this app processes events continuously:

```ruby
EventMeter.configure do |config|
  config.auto_cleanup_history = true
  config.cleanup_history_retention = 31 * 24 * 60 * 60
  config.cleanup_history_interval = 60 * 60
end
```

Automatic cleanup is off by default. With this setting, `process_pending`
periodically removes old processed data:

- minute and hour rollup buckets older than `cleanup_history_retention`
- interval state older than `cleanup_history_retention`
- leftover processed-entry markers from interrupted cleanup or failed stream deletion

The cleanup pass is guarded by the rollup storage lock and a storage-backed
watermark, so busy workers do not all clean on every call. Raw unprocessed
stream events are not deleted by age; they are removed only after they are
successfully processed. File stream quarantine retention is handled by
`time_bucket_stream` when that stream is read.

Cleanup settings:

| Setting | Default | Meaning |
| --- | --- | --- |
| `auto_cleanup_history` | `false` | When true, `process_pending` occasionally runs history cleanup. |
| `cleanup_history_retention` | 31 days | Processed report data older than this can be removed. |
| `cleanup_history_interval` | 1 hour | Minimum time between automatic cleanup passes. |
| `auto_cleanup_error_handler` | `warn` | Callable invoked when automatic cleanup fails without interrupting processing. |

Use a longer retention if you need longer report windows:

```ruby
config.cleanup_history_retention = 90 * 24 * 60 * 60
```

Use a custom cleanup error handler when cleanup failures should go to your app's
logger or error tracker:

```ruby
config.auto_cleanup_error_handler = ->(error) { Rails.logger.warn(error.message) }
```

Manual cleanup is also available:

```ruby
EventMeter.cleanup_history(before: Time.now.utc - 30 * 24 * 60 * 60)
```

Example output:

```ruby
{
  rollup_keys_deleted: 120,
  interval_state_keys_deleted: 7,
  processed_entries_deleted: 842
}
```

Clean only selected event names:

```ruby
EventMeter.cleanup_history(
  before: Time.now.utc - 30 * 24 * 60 * 60,
  events: ["invoice_delivery"]
)
```

File rollup processed sidecars live under one event/version folder, so file
cleanup can remove orphan sidecars for that report. Redis and PostgreSQL
processed-entry markers are also event-scoped, so `events:` can clean old
markers for only the selected event names. Redis markers also expire with
`rollup_ttl`.

In normal successful processing, processed-entry markers are deleted right after
their stream rows are deleted; cleanup is mainly a fallback for interrupted
cleanup or old retained data.

## Development

Redis and PostgreSQL clients are development dependencies, so the full test
suite can exercise real storage backends when services are available.

```sh
bundle install
bundle exec rake test
```

Redis integration tests use `EVENT_METER_REDIS_URL` when it is set, otherwise
they try the default local Redis connection:

```sh
EVENT_METER_REDIS_URL=redis://127.0.0.1:6379/0 bundle exec ruby -Itest test/storage_test.rb
```

PostgreSQL integration tests use `EVENT_METER_POSTGRES_URL` or `DATABASE_URL`
when either is set. Without those variables, they try a local
`postgres:///event_meter_test` database. If local PostgreSQL is reachable and
that database does not exist, the tests create it automatically.

Each test still creates isolated tables with a random prefix and drops those
tables afterward:

```sh
EVENT_METER_POSTGRES_URL=postgres://localhost/event_meter_test bundle exec ruby -Itest test/storage_test.rb
```

With a normal local PostgreSQL install, this also works:

```sh
bundle exec ruby -Itest test/storage_test.rb
```

The storage stress test runs every stream and rollup pairing. It writes from
multiple threads and forked processes, kills a processor after rollups are
written but before stream rows are deleted, then starts competing retry
processors and verifies the final report counts:

```sh
EVENT_METER_POSTGRES_URL=postgres://localhost/event_meter_test bundle exec ruby -Itest test/stress_test.rb
```

Increase the load when you want a longer local run:

```sh
EVENT_METER_STRESS_COUNT=5000 EVENT_METER_POSTGRES_URL=postgres://localhost/event_meter_test bundle exec ruby -Itest test/stress_test.rb
```

The lightweight performance checks are part of the default test task and can be
run directly when changing storage, cleanup, scan, or batch-processing code:

```sh
bundle exec rake performance
```

They use broad wall-clock budgets to catch accidental nonlinear behavior without
turning normal development into benchmarking theater. Print JSON timing samples:

```sh
EVENT_METER_PERFORMANCE_REPORT=1 bundle exec rake performance
```

Useful performance options:

| Option | Default | Meaning |
| --- | --- | --- |
| `EVENT_METER_PERFORMANCE_BATCH_EVENTS` | `4000` | Events in the large in-memory processor batch. |
| `EVENT_METER_PERFORMANCE_BATCH_SECONDS` | `4.0` | Time budget for processing that batch. |
| `EVENT_METER_PERFORMANCE_FILE_HISTORY_MINUTES` | `720` | File-store history buckets created before cleanup. |
| `EVENT_METER_PERFORMANCE_FILE_CLEANUP_SECONDS` | `6.0` | Time budget for file rollup cleanup. |
| `EVENT_METER_PERFORMANCE_REDIS_SCAN_KEYS` | `4000` | Old rollup, state, and processed-entry keys used by the Redis scan cleanup check. |
| `EVENT_METER_PERFORMANCE_REDIS_SCAN_SECONDS` | `3.0` | Time budget for Redis-style scan cleanup. |

Run the soak test when you want to watch memory, file handles, stream files,
rollup rows, and cleanup behavior over time. It is opt-in and is not part of
the default test task:

```sh
EVENT_METER_SOAK_SECONDS=30 bundle exec rake soak
```

Use PostgreSQL rollups:

```sh
EVENT_METER_SOAK_SECONDS=120 \
EVENT_METER_SOAK_STREAM=file \
EVENT_METER_SOAK_ROLLUP=postgres \
EVENT_METER_POSTGRES_URL=postgres://localhost/event_meter_test \
bundle exec rake soak
```

Useful soak options:

| Option | Default | Meaning |
| --- | --- | --- |
| `EVENT_METER_SOAK_SECONDS` | `10` | How long to keep appending and processing. |
| `EVENT_METER_SOAK_BATCH_SIZE` | `100` | Events written each loop. |
| `EVENT_METER_SOAK_SLEEP_SECONDS` | `0.05` | Pause between loops. |
| `EVENT_METER_SOAK_REPORT_SECONDS` | `5` | How often to print resource samples. |
| `EVENT_METER_SOAK_STREAM` | `file` | `file` or `redis`. |
| `EVENT_METER_SOAK_ROLLUP` | `postgres` when a PostgreSQL URL is present, otherwise `file` | `file`, `redis`, or `postgres`. |
| `EVENT_METER_SOAK_CUSTOMERS` | `100` | Distinct customer IDs used for interval metrics. |
| `EVENT_METER_SOAK_DELETE_FAIL_EVERY` | `0` | Simulate a stream-delete miss every N process runs. |
| `EVENT_METER_SOAK_CLEANUP_SECONDS` | `0` | Run `cleanup_history` every N seconds. |

The soak runner prints JSON lines for `soak_start`, `soak_sample`, and
`soak_finish`. The final report includes before/after resource snapshots and
deltas. It also fails if final summaries do not match the number of written
events.

## Public API

```ruby
EventMeter.configure
EventMeter.start
EventMeter.process_pending
EventMeter.summary
EventMeter.series
EventMeter.compare
EventMeter.report_definition
EventMeter.cleanup_history
```

Lifecycle event methods:

```ruby
event.success
event.skip
event.failure
```

Report definition methods:

```ruby
report.index_by
report.measure_interval_by
```
