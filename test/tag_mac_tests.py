#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tag_mac_tests.py - Automatically tag test cases that can run normally on macOS with @mac tag

Usage:
    python3 tag_mac_tests.py -d ./sql/test_scan --dry-run        # Preview mode
    python3 tag_mac_tests.py -d ./sql/test_scan                   # Actual execution
    python3 tag_mac_tests.py -d ./sql --concurrency 1             # Batch processing
    python3 tag_mac_tests.py -d ./sql --skip-existing             # Skip tests with @mac tag
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Set, Tuple
import json


class MacTestTagger:
    def __init__(self, test_dir: str, dry_run: bool = False,
                 skip_existing: bool = False, concurrency: int = 1,
                 case_filter: str = ".*"):
        self.test_dir = Path(test_dir).resolve()
        self.dry_run = dry_run
        self.skip_existing = skip_existing
        self.concurrency = concurrency
        self.case_filter = case_filter

        self.passed_tests: List[str] = []
        self.failed_tests: List[str] = []
        self.skipped_tests: List[str] = []
        self.tagged_files: List[str] = []

    def find_test_files(self) -> List[Path]:
        """Find all test case files (files under T directory)"""
        test_files = []

        # Find all test files under T directories
        for t_dir in self.test_dir.rglob("T"):
            if t_dir.is_dir():
                for file in t_dir.iterdir():
                    if file.is_file() and not file.name.startswith('.'):
                        test_files.append(file)

        return sorted(test_files)

    def has_mac_tag(self, file_path: Path) -> bool:
        """Check if file already has @mac tag"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                return '@mac' in first_line.lower()
        except Exception as e:
            print(f"❌ Cannot read file {file_path}: {e}")
            return False

    def get_test_name(self, file_path: Path) -> str:
        """Extract test name from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                # Match pattern: -- name: test_name @attr1 @attr2
                match = re.match(r'--\s*name:\s*(\S+)', first_line)
                if match:
                    return match.group(1)
                # If no name comment, use filename
                return file_path.stem
        except Exception as e:
            print(f"❌ Cannot extract test name {file_path}: {e}")
            return file_path.stem

    def run_test(self, test_file: Path) -> bool:
        """Run a single test and return whether it passes"""
        # Get test directory (parent directory containing T and R directories)
        test_suite_dir = test_file.parent.parent
        test_name = self.get_test_name(test_file)

        # Find test root directory (directory containing run.py)
        test_root = Path(__file__).parent.resolve()

        print(f"\n🧪 Running test: {test_name}")
        print(f"   File: {test_file.relative_to(test_root)}")

        # Build test command - test directory relative to test root
        cmd = [
            'python3', 'run.py',
            '-d', str(test_suite_dir.relative_to(test_root)),
            '--case_filter', f'^{re.escape(test_name)}$',
            '-c', str(self.concurrency),
            '--skip_reruns'  # Run once, no retry
        ]

        try:
            # Run test - cwd is test root directory
            result = subprocess.run(
                cmd,
                cwd=test_root,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes timeout
            )

            # Parse output to determine if test passed
            # nose test framework output format: OK means success, FAILED means failure
            output = result.stdout + result.stderr

            # Check if test passed
            if 'OK' in output and 'FAILED' not in output and result.returncode == 0:
                print(f"   ✅ Test passed")
                return True
            else:
                print(f"   ❌ Test failed")
                if self.dry_run:
                    print(f"   Output summary: {output[-500:]}")
                return False

        except subprocess.TimeoutExpired:
            print(f"   ⏱️  Test timeout")
            return False
        except Exception as e:
            print(f"   ❌ Error running test: {e}")
            return False

    def add_mac_tag(self, file_path: Path) -> bool:
        """Add @mac tag to the first line of file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if not lines:
                print(f"   ⚠️  File is empty: {file_path}")
                return False

            first_line = lines[0].rstrip()

            # Check if first line is name comment
            if first_line.startswith('--') and 'name:' in first_line:
                # If already has tags, append @mac at the end
                if '@' in first_line:
                    new_first_line = first_line + ' @mac\n'
                else:
                    new_first_line = first_line + ' @mac\n'
            else:
                # If no name comment, add one
                test_name = file_path.stem
                new_first_line = f'-- name: {test_name} @mac\n'
                lines.insert(0, new_first_line)
                new_first_line = lines[0]

            lines[0] = new_first_line

            if not self.dry_run:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)

            return True

        except Exception as e:
            print(f"   ❌ Failed to add tag {file_path}: {e}")
            return False

    def tag_test_file(self, test_file: Path):
        """Add @mac tag to test files (including T and R directories)"""
        test_root = Path(__file__).parent.resolve()

        # File in T directory
        t_file = test_file
        # File in R directory
        r_file = Path(str(test_file).replace('/T/', '/R/'))

        files_to_tag = [t_file]
        if r_file.exists():
            files_to_tag.append(r_file)

        for file in files_to_tag:
            if self.dry_run:
                print(f"   [DRY-RUN] Will add @mac tag: {file.relative_to(test_root)}")
            else:
                if self.add_mac_tag(file):
                    print(f"   ✅ Added @mac tag: {file.relative_to(test_root)}")
                    self.tagged_files.append(str(file.relative_to(test_root)))

    def process_tests(self):
        """Process all tests"""
        test_files = self.find_test_files()

        if not test_files:
            print(f"❌ No test files found in {self.test_dir}")
            return

        print(f"\n📊 Found {len(test_files)} test files")
        print(f"{'=' * 80}")

        for i, test_file in enumerate(test_files, 1):
            test_name = self.get_test_name(test_file)

            # Check if matches filter
            if not re.search(self.case_filter, test_name):
                continue

            print(f"\n[{i}/{len(test_files)}] Processing: {test_name}")

            # Check if already has @mac tag
            if self.has_mac_tag(test_file):
                print(f"   ⏭️  Already has @mac tag, skipping")
                self.skipped_tests.append(test_name)
                if self.skip_existing:
                    continue

            # Run test
            if self.run_test(test_file):
                self.passed_tests.append(test_name)
                # Add tag
                if not self.has_mac_tag(test_file):
                    self.tag_test_file(test_file)
            else:
                self.failed_tests.append(test_name)

        self.print_summary()

    def print_summary(self):
        """Print execution summary"""
        test_root = Path(__file__).parent.resolve()

        print(f"\n{'=' * 80}")
        print(f"📊 Execution Summary")
        print(f"{'=' * 80}")
        print(f"✅ Passed tests: {len(self.passed_tests)}")
        print(f"❌ Failed tests: {len(self.failed_tests)}")
        print(f"⏭️  Skipped tests: {len(self.skipped_tests)}")
        print(f"🏷️  Tagged files: {len(self.tagged_files)}")

        if self.dry_run:
            print(f"\n⚠️  This is DRY-RUN mode, no files were modified")

        # Save results to JSON file
        result = {
            'passed': self.passed_tests,
            'failed': self.failed_tests,
            'skipped': self.skipped_tests,
            'tagged_files': self.tagged_files
        }

        result_file = test_root / 'mac_test_results.json'
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"\n📄 Detailed results saved to: {result_file}")

        if self.failed_tests:
            print(f"\n❌ Failed test list:")
            for test in self.failed_tests:
                print(f"   - {test}")


def main():
    parser = argparse.ArgumentParser(
        description='Automatically tag test cases that can run normally on macOS with @mac tag',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview mode (no actual file modifications)
  python3 tag_mac_tests.py -d ./sql/test_scan --dry-run

  # Actual execution, tag tests in test_scan directory
  python3 tag_mac_tests.py -d ./sql/test_scan

  # Batch process all tests
  python3 tag_mac_tests.py -d ./sql --concurrency 1

  # Skip tests with @mac tag
  python3 tag_mac_tests.py -d ./sql/test_scan --skip-existing

  # Only process specific tests
  python3 tag_mac_tests.py -d ./sql/test_scan --case-filter "test_pushdown.*"
        """
    )

    parser.add_argument('-d', '--dir', required=True,
                        help='Test directory path (relative to test directory)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview mode, no actual file modifications')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip tests with @mac tag')
    parser.add_argument('-c', '--concurrency', type=int, default=1,
                        help='Concurrency level, default is 1')
    parser.add_argument('--case-filter', default='.*',
                        help='Test case name filter (regex), default is all tests')

    args = parser.parse_args()

    # Get script directory (test directory)
    script_dir = Path(__file__).parent.resolve()
    test_dir = script_dir / args.dir

    if not test_dir.exists():
        print(f"❌ Directory does not exist: {test_dir}")
        sys.exit(1)

    print(f"🚀 Start processing test directory: {test_dir}")
    print(f"   Dry-run mode: {args.dry_run}")
    print(f"   Skip existing tags: {args.skip_existing}")
    print(f"   Concurrency: {args.concurrency}")
    print(f"   Test filter: {args.case_filter}")

    tagger = MacTestTagger(
        test_dir=test_dir,
        dry_run=args.dry_run,
        skip_existing=args.skip_existing,
        concurrency=args.concurrency,
        case_filter=args.case_filter
    )

    try:
        tagger.process_tests()
    except KeyboardInterrupt:
        print("\n\n⚠️  User interrupted")
        tagger.print_summary()
        sys.exit(1)


if __name__ == '__main__':
    main()
