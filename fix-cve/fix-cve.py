#!/usr/bin/env python3

import glob
import sys
import os
import shutil
import subprocess


def get_pom_files():
    pom_files = []
    for f in glob.glob("pom/**/pom.properties", recursive=True):
        pom_files.append(f.split("/", maxsplit=1)[1])
    return pom_files


def patch_jar_file(jar_file, pom_files):
    jar_file = os.path.abspath(jar_file)
    new_jar_file = jar_file.replace(".jar", "-new.jar")
    print("=" * 80)
    print(f"Patch {jar_file}, new file: {new_jar_file}")
    cwd = os.getcwd()

    tmp_dir = "tmp/" + os.path.basename(jar_file)
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir, exist_ok=True)
        os.chdir(tmp_dir)
        ret = subprocess.run(["jar", "xf", jar_file], stdout=None, stderr=None)
        if not ret.returncode == 0:
            raise Exception(f"Failed to extract {jar_file}")

        for pom_file in pom_files:
            pom_in_jar = os.path.join("META-INF", "maven", pom_file)
            if os.path.exists(pom_in_jar):
                print(f"Copy {pom_file} to {pom_in_jar}")
                shutil.copy(os.path.join(cwd, "pom", pom_file), pom_in_jar)

        ret = subprocess.run(["jar", "cf", new_jar_file, "."], stdout=None, stderr=None)
        if not ret.returncode == 0:
            raise Exception(f"Failed to update {jar_file}")

        os.remove(jar_file)
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def patch_jars(output_dir, pom_files):
    fixed_jars = [
        "kudu-client-1.17.1.jar",
        "paimon-bundle-1.0.1.jar",
        "bundle-2.29.52.jar",
        "hadoop-client-runtime-3.4.1.jar",
        # hudi related
        "hbase-protocol-shaded-2.4.13.jar",
        "htrace-core4-4.2.0-incubating.jar",
        "hbase-shaded-netty-4.1.1.jar",
    ]
    for f in glob.glob(output_dir + "/**/*.jar", recursive=True):
        if f.split("/")[-1] in fixed_jars:
            patch_jar_file(f, pom_files)


def main():
    output_dir = sys.argv[1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    pom_files = get_pom_files()
    print("Available pom files:")
    print(pom_files)
    patch_jars(output_dir, pom_files)


if __name__ == "__main__":
    main()
