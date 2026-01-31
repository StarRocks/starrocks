# build-support

Brief notes on helper scripts in this directory.

- `build-support/check-format.sh`: Check clang-format across `be/src` and `be/test` without modifying files. Usage: `bash build-support/check-format.sh`
- `build-support/clang-format-changed-check.sh`: Check clang-format only on C++ files changed since `origin/main` (falls back to full check if missing). Usage: `bash build-support/clang-format-changed-check.sh`
- `build-support/clang-format-changed.sh`: Apply clang-format to C++ files changed since `origin/main` (falls back to full format if missing). Usage: `bash build-support/clang-format-changed.sh`
- `build-support/clang-format.sh`: Apply clang-format across `be/src` and `be/test`. Usage: `bash build-support/clang-format.sh`
- `build-support/compile_time.sh`: Collect and report compile-time statistics (clang). Usage: `bash build-support/compile_time.sh`
- `build-support/format_changed_files.py`: Filter a list of changed files to C++ sources within target dirs. Usage: `python3 build-support/format_changed_files.py --help`
- `build-support/gen_build_version.py`: Generate build version metadata. Usage: `python3 build-support/gen_build_version.py --help`
- `build-support/gen_notice.py`: Generate NOTICE file content from bundled licenses. Usage: `python3 build-support/gen_notice.py --help`
- `build-support/lintutils.py`: Shared helpers for lint/format scripts. Imported by other scripts.
- `build-support/run_clang_format.py`: Run clang-format in check or fix mode for given dirs. Usage: `python3 build-support/run_clang_format.py --help`
- `build-support/sync_pom_to_gradle.py`: Sync Maven POM settings into Gradle config. Usage: `python3 build-support/sync_pom_to_gradle.py --help`
