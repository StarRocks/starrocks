# StarRocks Debian Packaging

This directory contains the scripts and metadata required to build standard `.deb` packages for StarRocks (FE, BE, and CN).

## Prerequisites
- **Bash 4.0+**
- **dpkg** (providing `dpkg-deb` version >= 1.19.0 for `--root-owner-group` support)
- **StarRocks build artifacts**: You must build StarRocks from source first to generate the `out/` directories.

## Building Packages
Run the build script from this directory:
`./build.sh <version> [path_to_fe_out] [path_to_be_out] [arch] [path_to_cn_out]`

**Example:**
`./build.sh 4.0.4 ../../out/fe ../../out/be amd64 ../../out/be`

## Installation
Install the generated packages using `apt` (to ensure dependencies like `libssl` and `libibverbs1` are resolved):
`sudo apt install ./starrocks-fe_4.0.4_amd64.deb`

> **Note:** `starrocks-be` and `starrocks-cn` are mutually exclusive and cannot be installed on the same host simultaneously.

## Service Management
The packages include systemd units for easy management:
- `sudo systemctl start starrocks-fe`
- `sudo systemctl enable starrocks-be`
- `sudo systemctl status starrocks-cn`

## Directory Structure
- `/etc/starrocks/<comp>`: Configuration files (Root-owned, 640/750 permissions)
- `/usr/lib/starrocks/<comp>`: Binaries and shared libraries
- `/var/lib/starrocks/<comp>`: Metadata (FE) or Data Storage (BE/CN)
- `/var/log/starrocks/<comp>`: Component-specific logs