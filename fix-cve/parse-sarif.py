#!/usr/bin/env python3

import json
import os

# to parse the sarif file and get the bundle and package names
# docker scout cves --format=sarif [IMAGE] > out.sarif


def parse_sarif(sarif_file):
    with open(sarif_file) as f:
        js = json.load(f)

    bundles = set()
    packages = set()

    for run in js["runs"]:
        for x in run["results"]:
            locations = x.get("locations", [])
            for x in locations:
                uri = (
                    x.get("physicalLocation", {}).get("artifactLocation", {}).get("uri")
                )
                if uri.startswith("/opt/starrocks"):
                    if ":" in uri:
                        artifact = uri.split(":")[0]
                        package = uri.split(":", maxsplit=1)[1]
                        packages.add(package)
                        bundles.add(artifact)

    print("=" * 10 + "Bundles" + "=" * 10)
    for p in bundles:
        print('"' + os.path.basename(p) + '",')

    print("=" * 10 + "Packages" + "=" * 10)
    for p in packages:
        print('("' + p + '", "fixed.version"),')


def main():
    import sys

    sarif_file = sys.argv[1] if len(sys.argv) > 1 else None
    if sarif_file:
        parse_sarif(sarif_file)


if __name__ == "__main__":
    main()
