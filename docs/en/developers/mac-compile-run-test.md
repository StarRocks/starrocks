---
displayed_sidebar: docs
---

# Compile, Run and Test StarRocks on macOS ARM64

This document provides detailed instructions on how to compile, run, debug, and test StarRocks on the macOS ARM64 platform (Apple Silicon), making it easier for developers to work on Mac.

## Prerequisites

### Software Requirements

- macOS 15.0 or higher
- Xcode Command Line Tools
- Homebrew package manager

### Install Basic Tools

1. Install Xcode Command Line Tools:

```bash
xcode-select --install
```

2. Install Homebrew (if not already installed):

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

3. Install necessary Homebrew dependencies:

```bash
cd build-mac
./env_macos.sh
```

## Mac Compilation

## Compile Third-party Libraries

Before compiling BE on macOS, you need to compile third-party dependencies first. Navigate to the `build-mac` directory and run:

```bash
cd build-mac
./build_thirdparty.sh
```

## Compile FE

### Compilation Steps

```bash
./build.sh --fe
```

### Compilation Output

After compilation, FE artifacts are located in:

```
output/fe/
├── bin/              # FE startup scripts
├── conf/             # Configuration files
├── lib/              # JAR packages and dependencies
└── meta/             # Metadata storage directory
```

## Compile BE

The BE compilation script for macOS is located in the `build-mac` directory.

### First-time Compilation

```bash
cd build-mac
./build_be.sh
```

The first-time compilation will automatically complete the following steps:

1. Check and configure environment variables
2. Compile third-party dependencies (protobuf, thrift, brpc, etc.)
3. Generate code (Thrift/Protobuf)
4. Compile BE code
5. Install to `be/output/` directory

### Compilation Output

After compilation, BE artifacts are located in:

```
be/output/
├── bin/              # Startup scripts
├── conf/             # Configuration files
├── lib/              # starrocks_be binary file
│   └── starrocks_be  # BE main program
└── storage/          # Data storage directory
```

## Configure and Run FE

### Configure FE

Edit the `output/fe/conf/fe.conf` file:

```properties
# Network configuration - Specify the network range for FE to listen on
# Adjust according to your local network configuration
priority_networks = 10.10.10.0/24;192.168.0.0/16

# Set default replica count to 1 (for single-machine development environment)
default_replication_num = 1

# Reset election group (IP changes during compilation affect FE leader election)
bdbje_reset_election_group = true
```

### Start FE

```bash
cd output/fe
./bin/start_fe.sh --daemon
```

### View FE Logs

```bash
tail -f output/fe/log/fe.log
```

## Configure and Run BE

### Configure BE

Edit the `be/output/conf/be.conf` file:

```properties
# Network configuration - Must match FE configuration
priority_networks = 10.10.10.0/24;192.168.0.0/16

# Disable datacache (not supported on macOS)
datacache_enable = false

# Disable system metrics collection (some features not supported on macOS)
enable_system_metrics = false
enable_table_metrics = false
enable_collect_table_metrics = false

# Verbose logging mode (for debugging)
sys_log_verbose_modules = *
```

### Start BE

On macOS, starting BE requires setting some environment variables:

```bash
cd be/output

# Set environment variables and start BE
export ASAN_OPTIONS=detect_container_overflow=0
export STARROCKS_HOME=/Users/kks/git/starrocks/be/output
export PID_DIR=/Users/kks/git/starrocks/be/output/bin
export UDF_RUNTIME_DIR=/Users/kks/git/starrocks/be/output/lib

# Start BE in background
./lib/starrocks_be &
```

> **Note**: Replace the path `/Users/kks/git/starrocks` with your actual StarRocks code path.

### View BE Logs

```bash
tail -f be/output/log/be.INFO
```

### Stop BE

```bash
ps aux | grep starrocks_be

kill -9 <PID>
```

### Add BE to Cluster

After starting BE, you need to register it to FE via MySQL client:

```sql
-- Connect to FE
mysql -h 127.0.0.1 -P 9030 -u root

-- Add BE (replace with your actual IP address)
ALTER SYSTEM ADD BACKEND "127.0.0.1:9050";

-- Check BE status
SHOW BACKENDS\G
```

Confirm that the `Alive` field is `true`, indicating BE is running properly.

## Connect and Test

### Connect Using MySQL Client

```bash
# Use MySQL client installed via Homebrew
/opt/homebrew/Cellar/mysql-client/9.4.0/bin/mysql -h 127.0.0.1 -P 9030 -u root

# Or use system path (if configured)
mysql -h 127.0.0.1 -P 9030 -u root
```

### Basic Testing

```sql
-- Create database
CREATE DATABASE test_db;
USE test_db;

-- Create table
CREATE TABLE test_table (
    id INT,
    name VARCHAR(100)
) DISTRIBUTED BY HASH(id) BUCKETS 1;

-- Insert data
INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');

-- Query data
SELECT * FROM test_table;
```

## Debug BE

### Using LLDB for Debugging

macOS uses LLDB as the debugger (replacement for GDB).

#### Attach to Running BE Process

```bash
# Find BE process ID
ps aux | grep starrocks_be

# Attach to process using LLDB (replace <PID> with actual process ID)
sudo lldb -p <PID>
```

#### Common LLDB Commands

```lldb
# View all threads
thread list

# View current thread's stack
bt

# View all threads' stacks
thread backtrace all

# Continue execution
continue

# Step execution
step

# Set breakpoint
breakpoint set --name function_name

# Exit LLDB
quit
```

#### Export All Thread Stacks to File

```bash
# Export stack information for problem analysis
lldb -p <PID> --batch -o "thread backtrace all" > starrocks_bt.txt
```

## SQL Testing

StarRocks provides a complete SQL testing framework that can run on macOS.

### Prepare Test Environment

```bash
# Navigate to test directory
cd test

# Create Python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install test dependencies
python3 -m pip install -r requirements.txt
```

### Mark macOS Compatible Test Cases

Since the macOS version has some features disabled, you need to mark which test cases can run on Mac:

```bash
# Mark tests in test_scan directory (preview mode, no actual modification)
python3 tag_mac_tests.py -d ./sql/test_scan --dry-run

# Actually mark tests
python3 tag_mac_tests.py -d ./sql/test_scan

# Mark tests in test_agg directory
python3 tag_mac_tests.py -d ./sql/test_agg
```

### Run SQL Tests

```bash
# Run tests with @mac tag
pytest sql/test_scan -m mac

# Run specific test file
pytest sql/test_scan/test_basic_scan.py -m mac

# Verbose output
pytest sql/test_agg -m mac -v
```

### Test Configuration

The test framework will automatically connect to the local StarRocks instance:

- Host: `127.0.0.1`
- Port: `9030`
- User: `root`
- Password: empty

## Development Tools Configuration

### Git Pre-commit Hook (Code Formatting)

To maintain consistent code style, it's recommended to configure a pre-commit hook to automatically format C++ code.

Create the file `.git/hooks/pre-commit`:

```bash
#!/bin/bash

echo "Running clang-format on modified files..."

STYLE=$(git config --get hooks.clangformat.style)
if [ -n "${STYLE}" ] ; then
  STYLEARG="-style=${STYLE}"
else
  STYLEARG=""
fi

format_file() {
  file="${1}"
  if [ -f $file ]; then
    /opt/homebrew/Cellar/clang-format@11/11.1.0/bin/clang-format-11 -i ${STYLEARG} ${1}
    git add ${1}
  fi
}

case "${1}" in
  --about )
    echo "Runs clang-format on source files"
    ;;
  * )
    for file in `git diff-index --cached --name-only HEAD | grep -iE '\.(cpp|cc|h|hpp)$' ` ; do
      format_file "${file}"
    done
    ;;
esac
```

Add execute permissions:

```bash
chmod +x .git/hooks/pre-commit
```

## Limitations and Known Issues

### Features Disabled in macOS Version

Due to platform compatibility limitations, the following features are disabled in the macOS version:

#### Storage and Data Sources

- **HDFS Support**: No HDFS client library available
- **ORC Format**: Complete ORC module disabled
- **Avro Format**: avro-c library compatibility issues
- **JDBC Data Source**: JNI/Java dependencies
- **MySQL Scanner**: Missing MariaDB development headers

#### Cloud Service Integration

- **AWS S3**: Poco HTTP client compatibility issues
- **Azure Storage**: Azure SDK compatibility issues
- **Apache Pulsar**: librdkafka++ compatibility issues

#### Performance and Monitoring Tools

- **Google Breakpad**: Crash reporting (minidump)
- **OpenTelemetry**: Distributed tracing

#### Other Features

- **GEO Module**: Missing generated parser files
- **JIT Compilation**: Requires complete LLVM development environment
- **Starcache**: Cache acceleration feature
- **CLucene**: Inverted index functionality
- **LZO Compression**: Use alternative compression algorithms

### Design Principles

The macOS compilation implementation follows these principles:

1. **Do Not Affect Linux Compilation**: All modifications are isolated through conditional compilation
2. **Minimize Code Changes**: Prefer disabling features through configuration
3. **Centralized Management**: Mac-related modifications are centralized in the `build-mac/` directory
4. **Use Standard Tools**: Rely on Homebrew and LLVM toolchain
5. **Compile Key Dependencies from Source**: Ensure ABI compatibility (protobuf, thrift, brpc)



## FAQ

**Q: Getting "protobuf version mismatch" error during compilation**

A: Make sure to use `thirdparty/installed/bin/protoc` (version 3.14.0) instead of system or Homebrew's protobuf:

```bash
# Check protobuf version
/Users/kks/git/starrocks/thirdparty/installed/bin/protoc --version

# Should output: libprotoc 3.14.0
```

**Q: Out of memory during compilation**

A: Reduce the number of parallel compilation tasks:

```bash
./build_be.sh --parallel 4
```

**Q: FE cannot connect to BE**

A: Ensure that the `priority_networks` configuration in FE and BE is consistent and includes your local IP address.

**Q: Some test cases fail**

A: Check if they are tests related to disabled features (such as HDFS, S3, ORC, etc.), as these tests cannot pass on macOS.

## Contribution Guidelines

If you find issues or have improvement suggestions on macOS:

1. Check the relevant scripts in the `build-mac/` directory
2. Follow the principle of "do not affect Linux compilation"
3. Use `#ifdef __APPLE__` for platform-specific code modifications
4. Submit a Pull Request with detailed description of changes

Contributions are welcome in the following areas:

- Support compiling more disabled features on Mac
- Continuously improve the @mac tag for SQL tests

---

Happy coding on macOS!
