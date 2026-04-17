# ADBC Smoke Tests

Throwaway tests proving the ADBC scanner stack works E2E against SQLite.

## Prerequisites

1. Build thirdparty with ADBC (produces `libadbc_driver_sqlite.so`):
   ```bash
   cd thirdparty && ./build-thirdparty.sh adbc
   ```

2. Build BE in Debug or ASAN mode:
   ```bash
   # Standard debug build
   BUILD_TYPE=Debug ./build.sh --be

   # ASan build (memory safety)
   BUILD_TYPE=ASAN ./build.sh --be

   # TSan build (thread safety)
   BUILD_TYPE=TSAN ./build.sh --be
   ```

## Running

```bash
# From repo root — the test auto-discovers the SQLite driver from thirdparty/installed/
./run-be-ut.sh --build-target adbc_smoke_test --module adbc_smoke_test --without-java-ext

# Or run the binary directly after building:
./output/be/test/exec/adbc_smoke_test
```

### Override driver path

If the SQLite driver is installed elsewhere:
```bash
ADBC_SQLITE_DRIVER_PATH=/path/to/libadbc_driver_sqlite.so ./output/be/test/exec/adbc_smoke_test
```

Driver path resolution order:
1. `ADBC_SQLITE_DRIVER_PATH` env var
2. `thirdparty/installed/lib64/libadbc_driver_sqlite.so`
3. `~/.config/adbc/drivers/sqlite_linux_amd64_v1.10.0/libadbc_driver_sqlite.so`

Tests skip with `GTEST_SKIP` if no driver is found.

## Test cases

| Test | What it proves |
|------|---------------|
| `DriverRegistrySingleton` | get_or_load returns same pointer, loaded_count=1 |
| `ScannerInitViaDriverManager` | Database+connection+statement via driver manager |
| `EndToEndSelect` | CREATE TABLE, INSERT, SELECT with row verification |
| `ConcurrentScan` | 8 threads with per-thread connections, 100 rows each |

## Running under sanitizers

```bash
# ASan (memory leaks, use-after-free)
BUILD_TYPE=ASAN ./build.sh --be
./output/be/test/exec/adbc_smoke_test

# TSan (data races) — ConcurrentScan is the key test
BUILD_TYPE=TSAN ./build.sh --be
./output/be/test/exec/adbc_smoke_test --gtest_filter=ADBCSmokeTest.ConcurrentScan
```
