# BramptonTransitAnalysis (!!!Work In Progress!!!)

A long-running Python data collection pipeline that pulls real-time and static GTFS data from Brampton Transit, stores it in a local SQLite database, and exports daily snapshots to CSV. Designed to run continuously on a local machine or server.

---

## What it does

Three concurrent threads run independently on a shared `stop_event` signal:

| Thread | Schedule | Responsibility |
|---|---|---|
| `DataCollector` | Every 15 seconds | Polls the GTFS-RT feed for live bus positions and writes new records to SQLite |
| `GTFSDownloader` | 12:30 PM daily | Downloads the latest GTFS static feed (routes, stops, trips, stop times) and loads any new feed version into the database |
| `DataExporter` | 2:30 AM daily | Exports the previous day's bus location data to CSV, cleans up old GTFS table data, and vacuums the database |

All three threads write structured logs to a separate `LogStorage.db` via a shared logger, so the main database is never used for diagnostics.

---

## Architecture

![Main Architecture](https://github.com/renacin/BramptonTransitAnalysis/blob/master/Misc/brampton_transit_architecture.png)

**Deduplication:** The `U_ID_TEMP` table caches the last 5 minutes of unique IDs (composite of route, vehicle, and timestamp). Each collection cycle checks against this cache before inserting, preventing duplicate rows from overlapping API polls.

**Route speed calculation:** On startup and after each new GTFS feed version, stop-to-stop distances are computed using vectorised haversine and joined with scheduled arrival times to derive average trip speed and average travel speed per `trip_id`. Results are stored in `ROUTE_SPEED`.

---

## Requirements

- Python 3.9+
- pip packages:

```
requests
pandas
numpy
```

Install with:

```bash
pip install requests pandas numpy
```

No external database server required — everything runs on SQLite.

---

## Setup

1. Clone the repository:

```bash
git clone https://github.com/renacin/BramptonTransitAnalysis.git
cd BramptonTransitAnalysis
```

2. Verify your system has a `Downloads` folder at the default path (`~/Downloads`). The pipeline creates its working directories there automatically:

```
~/Downloads/BramptonTransitAnalysis/
    3_Data/       ← DataStorage.db, LogStorage.db
    4_Storage/    ← CSV exports, GTFS archive
```

3. Run:

```bash
python main.py
```

On first run, the pipeline will:
- Create all output directories
- Download the current GTFS static feed
- Compute initial route speed estimates
- Begin collecting bus positions every 15 seconds

Stop with `Ctrl+C`. The main thread catches `KeyboardInterrupt`, sets the shared stop event, and waits up to 30 seconds for all worker threads to exit cleanly.

---

## Output

### `BUS_LOC_DB` table / daily CSV

One row per unique bus observation. Key fields:

| Column | Description |
|---|---|
| `u_id` | Composite unique ID (`route_id + vehicle_id + timestamp`) |
| `trip_route_id` | Route identifier |
| `vehicle_id` | Vehicle identifier |
| `position_latitude` | Latitude |
| `position_longitude` | Longitude |
| `position_bearing` | Heading in degrees |
| `position_speed` | Reported speed |
| `timestamp` | Unix epoch from the GTFS-RT feed |
| `dt_colc` | Human-readable datetime, converted to Eastern time |

CSV files are named `BUS_LOC_DB_DD-MM-YYYY.csv` and written to `4_Storage/BUS_LOC_DB/`.

### `ROUTE_SPEED` table

Derived from GTFS static data. One row per `trip_id` per feed version:

| Column | Description |
|---|---|
| `trip_id` | GTFS trip identifier |
| `tot_dist` | Total trip distance in km |
| `tot_idle_time` | Cumulative dwell time at stops (seconds) |
| `tot_trvl_time` | Cumulative time moving between stops (seconds) |
| `avg_trip_speed` | km/h averaged over total trip time |
| `avg_trvl_speed` | km/h averaged over moving time only |
| `feed_version` | GTFS feed version this row was derived from |

---

## Project structure

```
BramptonTransitAnalysis/
├── main.py                  # Entry point, thread orchestration, scheduler logic
└── Functions/
    ├── env_config.py        # All configuration (URLs, paths, table schemas)
    ├── env_setup.py         # First-run setup: folders, database tables, route speeds
    ├── data_collect.py      # DataCollector class — GTFS-RT polling and dedup logic
    ├── data_exporter.py     # DataExporter class — CSV export and database cleanup
    ├── gtfs_downloader.py   # GTFSDownloader class — static GTFS fetch and load
    └── data_helper.py       # Shared utilities: haversine distance, shared logger
```

---

## Configuration

All runtime settings live in `Functions/env_config.py`:

| Setting | Default | Description |
|---|---|---|
| `BUS_LOC_URL` | Brampton Transit GTFS-RT endpoint | Live bus position feed URL |
| `GTFS_URL` | ArcGIS item data URL | Static GTFS zip download URL |
| `cache_time_limit` | `5` (minutes) | How long to retain UIDs in the dedup cache |
| `timeout_time` | `10` (seconds) | Request timeout for external API calls |

Output paths are constructed relative to `~/Downloads/BramptonTransitAnalysis/` and require no changes for standard macOS or Linux setups.

---

## Notes

- The pipeline is designed for personal or research use. Brampton Transit's GTFS-RT feed is publicly accessible but rate-limited — polling every 15 seconds is intentional to stay within acceptable usage.
- SQLite WAL mode is enabled on both databases, allowing the three threads to read and write concurrently without blocking each other.
- The `DataExporter` keeps the two most recent GTFS feed versions in the database and archives older versions to CSV before deleting them. This preserves historical schedule data without growing the database indefinitely.
