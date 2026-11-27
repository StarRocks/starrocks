#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise a built CCTZ library against deterministic TZif v3 fixtures.

Usage: python3 build-support/test_cctz_all_year_dst.py \
    --source /path/to/cctz-2.3 --library /path/to/libcctz.a

Uses CXX/CXXFLAGS for the small probe. No downloads or host tzdata required.
The unpatched 2.3 library must fail the year-round DST cases; the patched
library must pass all cases. Fixtures model the reported footer and selected
transitions, rather than claiming to reproduce an entire distribution tzdb.
"""

import argparse
import datetime
import os
from pathlib import Path
import shlex
import struct
import subprocess
import sys
import tempfile


PROBE = r"""
#include <cctz/time_zone.h>
#include <cstdlib>
#include <iostream>
int main(int argc, char** argv) {
  cctz::time_zone tz;
  if (!cctz::load_time_zone(argv[1], &tz)) {
    std::cout << "unloaded\n";
    return 0;
  }
  std::cout << "loaded\n";
  for (int i = 2; i < argc; ++i) {
    const cctz::time_point<cctz::seconds> tp{
        cctz::seconds(std::strtoll(argv[i], nullptr, 10))};
    const auto al = tz.lookup(tp);
    const bool roundtrip = cctz::convert(al.cs, tz) == tp;
    std::cout << al.offset << " " << al.is_dst << " " << roundtrip << "\n";
  }
}
"""


def epoch(value):
    return int(datetime.datetime.fromisoformat(value).replace(
        tzinfo=datetime.timezone.utc).timestamp())


def tzif(transitions, types, footer):
    """types contains (UTC offset, DST flag, abbreviation) tuples."""
    abbreviations = b""
    records = b""
    for offset, dst, abbr in types:
        records += struct.pack(">lBB", offset, dst, len(abbreviations))
        abbreviations += abbr.encode("ascii") + b"\0"
    header = b"TZif3" + b"\0" * 15 + struct.pack(
        ">6I", 0, 0, 0, len(transitions), len(types), len(abbreviations))

    def block(fmt):
        return (b"".join(struct.pack(fmt, t) for t, _ in transitions)
                + bytes(i for _, i in transitions) + records + abbreviations)

    return header + block(">l") + header + block(">q") + b"\n" + footer.encode() + b"\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--library", required=True, type=Path)
    args = parser.parse_args()
    cases = []

    def add(name, transitions, types, footer, samples, loaded=True):
        cases.append((name, tzif(transitions, types, footer), samples, loaded))

    footer = "<+00>0<+01>,0/0,J365/25"
    types = [(0, 0, "+00"), (3600, 1, "+01")]
    transitions = [(epoch("2024-01-01"), 1),
                   (epoch("2025-02-23T02:00:00"), 0),
                   (epoch("2025-04-06T02:00:00"), 1)]
    # Avoid the repeated local hour for the reverse-conversion assertions.
    samples = [("2025-01-01", 3600, 1), ("2025-03-15", 0, 0),
               ("2025-04-06T02:00:00", 3600, 1),
               ("2025-12-31T23:00:00", 3600, 1),
               ("2028-02-29", 3600, 1), ("2028-12-31T23:00:00", 3600, 1),
               ("2500-01-01", 3600, 1)]
    add("Casablanca", transitions, types, footer, samples)
    add("El_Aaiun", transitions, types, footer, samples)
    add("NegativeDST", [(0, 0), (86400, 1)],
        [(2400, 0, "XXX"), (1200, 1, "+01")],
        "XXX-0:40<+01>-0:20,0/0,J365/23:40",
        [("2028-01-01", 1200, 1), ("2500-01-01", 1200, 1)])
    add("BeforeEpoch", [(-86400, 1)], types, footer,
        [("2025-01-01", 3600, 1), ("2500-01-01", 3600, 1)])
    add("NoTransitions", [], [(3600, 1, "+01")], footer,
        [("2025-01-01", 3600, 1), ("2500-01-01", 3600, 1)])
    add("Shanghai", [], [(28800, 0, "CST")], "CST-8",
        [("2025-03-15", 28800, 0), ("2500-01-01", 28800, 0)])
    add("FixedUTC", [], [(0, 0, "UTC")], "UTC0", [("2025-03-15", 0, 0)])
    add("Seasonal", [(epoch("2024-03-10T07:00:00"), 1),
                     (epoch("2024-11-03T06:00:00"), 0)],
        [(-18000, 0, "EST"), (-14400, 1, "EDT")],
        "EST5EDT,M3.2.0/2,M11.1.0/2",
        [("2025-01-01", -18000, 0), ("2025-07-01", -14400, 1),
         ("2500-07-01", -14400, 1)])
    for name, invalid_type in [("WrongOffset", (7200, 1, "+01")),
                               ("WrongDST", (3600, 0, "+01")),
                               ("WrongAbbreviation", (3600, 1, "BAD"))]:
        add(name, [(0, 0), (86400, 1)], [types[0], invalid_type],
            footer, [], loaded=False)

    failures = []
    with tempfile.TemporaryDirectory(prefix="cctz-dst-") as tmp:
        root = Path(tmp)
        probe_source = root / "probe.cc"
        probe_source.write_text(PROBE)
        probe = root / "probe"
        command = (shlex.split(os.environ.get("CXX", "c++"))
                   + shlex.split(os.environ.get("CXXFLAGS", ""))
                   + ["-std=c++11", "-I" + str(args.source.resolve() / "include"),
                      str(probe_source), str(args.library.resolve()), "-pthread"])
        if sys.platform == "darwin":
            command += ["-framework", "CoreFoundation"]
        subprocess.run(command + ["-o", str(probe)], check=True)
        for name, data, samples, loaded in cases:
            (root / name).write_bytes(data)
            result = subprocess.run(
                [str(probe), name] + [str(epoch(s[0])) for s in samples],
                env={**os.environ, "TZDIR": str(root)}, text=True,
                capture_output=True, check=True)
            expected = ["loaded" if loaded else "unloaded"]
            expected += [f"{offset} {dst} 1" for _, offset, dst in samples]
            if result.stdout.splitlines() != expected:
                failures.append(name)
                print(f"FAIL {name}: expected {expected}, got {result.stdout!r}")
            else:
                print(f"PASS {name}")
    print(f"{len(cases) - len(failures)}/{len(cases)} cases passed")
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
