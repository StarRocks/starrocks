#!/usr/bin/env python3

import glob
import sys
import os
import shutil
import subprocess

fixed_poms = [
    ("com.fasterxml.jackson.core:jackson-databind", "2.18.3"),
    ("io.netty:netty-handler", "4.1.118.Final"),
    ("io.netty:netty-codec-http2", "4.1.118.Final"),
    ("io.netty:netty-codec-http", "4.1.118.Final"),
    ("io.netty:netty-codec-haproxy", "4.1.118.Final"),
    ("io.netty:netty-common", "4.1.118.Final"),
    ("io.netty:netty-all", "4.1.118.Final"),
    ("com.google.protobuf:protobuf-java", "3.25.5"),
    ("org.apache.avro:avro", "1.11.4"),
    ("org.apache.commons:commons-compress", "1.26.0"),
    ("com.google.guava:guava", "32.0.1-jre"),
    ("org.eclipse.jetty:jetty-webapp", "10.0.24"),
    ("org.eclipse.jetty:jetty-server", "10.0.24"),
    ("org.eclipse.jetty:jetty-http", "10.0.24"),
    ("org.eclipse.jetty:jetty-xml", "10.0.24"),
    ("org.eclipse.jetty:jetty-io", "10.0.24"),
]

fixed_jars = [
    # common jars
    "kudu-client-1.17.1.jar",
    "paimon-bundle-1.0.1.jar",
    "bundle-2.29.52.jar",
    "hadoop-client-runtime-3.4.1.jar",
    # hudi related jars
    "hbase-protocol-shaded-2.4.13.jar",
    "htrace-core4-4.2.0-incubating.jar",
    "hbase-shaded-netty-4.1.1.jar",
    "hbase-shaded-jetty-4.1.1.jar",
    "hbase-shaded-miscellaneous-4.1.1.jar",
    # extra scan from docker scout
    "spark-network-common_2.12-3.5.5.jar",
    "spark-core_2.12-3.5.5.jar",
]


def write_pom_files():
    for ga, version in fixed_poms:
        g, a = ga.split(":")
        os.makedirs(f"pom/{g}/{a}", exist_ok=True)
        with open(f"pom/{g}/{a}/pom.properties", "w") as f:
            f.write(f"groupId={g}\nartifactId={a}\nversion={version}\n")


def get_pom_files():
    pom_files = []
    for f in glob.glob("pom/**/pom.properties", recursive=True):
        pom_files.append(f.split("/", maxsplit=1)[1])
    return pom_files


def patch_jar_file(jar_file, pom_files):
    jar_file = os.path.abspath(jar_file)
    new_jar_file = jar_file.replace(".jar", "-cve-patched.jar")
    print("=" * 80)
    print(f"Patch {jar_file}, new file: {new_jar_file}")
    cwd = os.getcwd()

    work_dir = "cve-patched/" + os.path.basename(jar_file)
    try:
        shutil.rmtree(work_dir, ignore_errors=True)
        os.makedirs(work_dir, exist_ok=True)
        os.chdir(work_dir)
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
        shutil.rmtree(work_dir, ignore_errors=True)


def patch_jars(output_dir, pom_files):
    for f in glob.glob(output_dir + "/**/*.jar", recursive=True):
        if f.split("/")[-1] in fixed_jars:
            patch_jar_file(f, pom_files)


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    write_pom_files()
    pom_files = get_pom_files()
    print("Available pom files:")
    print(pom_files)

    if output_dir:
        print('Patch jars in "{}"'.format(output_dir))
        patch_jars(output_dir, pom_files)


if __name__ == "__main__":
    main()
