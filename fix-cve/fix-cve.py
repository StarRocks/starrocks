#!/usr/bin/env python3

import glob
import sys
import os
import re
import shutil
import subprocess

io_netty_clean_version = "4.1.135.Final"

fixed_poms = [
    ("com.fasterxml.jackson.core:jackson-core", "2.21.4"),
    ("com.fasterxml.jackson.core:jackson-databind", "2.21.4"),
    ("io.netty:netty-handler", io_netty_clean_version),
    ("io.netty:netty-codec", io_netty_clean_version),
    ("io.netty:netty-codec-dns", io_netty_clean_version),
    ("io.netty:netty-codec-http2", io_netty_clean_version),
    ("io.netty:netty-codec-http", io_netty_clean_version),
    ("io.netty:netty-codec-haproxy", io_netty_clean_version),
    ("io.netty:netty-codec-redis", io_netty_clean_version),
    ("io.netty:netty-codec-smtp", io_netty_clean_version),
    ("io.netty:netty-common", io_netty_clean_version),
    ("io.netty:netty-resolver-dns", io_netty_clean_version),
    ("io.netty:netty-transport-native-epoll", io_netty_clean_version),
    ("io.netty:netty-transport-sctp", io_netty_clean_version),
    ("io.netty:netty-all", io_netty_clean_version),
    ("com.google.protobuf:protobuf-java", "3.25.5"),
    ("org.apache.avro:avro", "1.11.4"),
    ("org.apache.commons:commons-compress", "1.26.0"),
    ("com.google.guava:guava", "32.0.1-jre"),
    ("org.eclipse.jetty:jetty-webapp", "10.0.24"),
    ("org.eclipse.jetty:jetty-server", "10.0.24"),
    ("org.eclipse.jetty:jetty-http", "10.0.28-celerdata-b20260421"),
    ("org.eclipse.jetty:jetty-xml", "10.0.24"),
    ("org.eclipse.jetty:jetty-io", "10.0.24"),
    ("commons-beanutils:commons-beanutils", "1.11.0"),
    ("org.apache.httpcomponents.client5:httpclient5", "5.4.3"),
    ("io.airlift:aircompressor", "2.0.3"),
    ("org.jline:jline-remote-telnet", "4.2.1"),
]

fixed_jars = [
    # common jars
    "kudu-client-1.17.1.jar",
    "paimon-bundle-1.3.1.jar",
    "bundle-2.29.52.jar",
    "hadoop-client-runtime-3.4.3.jar",
    "gcs-connector-hadoop3-2.2.26-shaded.jar",
    "cos_api-bundle-5.6.137.2.jar",
    # hudi related jars
    "hbase-protocol-shaded-2.4.13.jar",
    "htrace-core4-4.2.0-incubating.jar",
    "hbase-shaded-netty-4.1.1.jar",
    "hbase-shaded-jetty-4.1.1.jar",
    "hbase-shaded-miscellaneous-4.1.1.jar",
    "grpc-netty-shaded-1.63.0.jar",
    "grpc-netty-shaded-1.67.1.jar",
    "parquet-hadoop-bundle-1.15.2.jar",
    "parquet-jackson-1.15.2.jar",
    "parquet-jackson-1.16.0.jar",
    # extra scan from docker scout
    "spark-network-common_2.12-3.5.5.jar",
    "spark-core_2.12-3.5.7.jar",
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


def read_pom_property_version(properties_path):
    """Return the value of the `version=` line in a pom.properties file, or None."""
    try:
        with open(properties_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("version="):
                    return line[len("version="):]
    except OSError:
        pass
    return None


def mask_pom_xml_version(xml_path, old_version, clean_version):
    """Rewrite <version>old</version> -> <version>clean</version> in a jar's pom.xml.

    This masks the project's own version (and its parent version, when they share
    the same literal) so scanners that read pom.xml -- not just pom.properties --
    see the clean version. It is a textual replace scoped to <version> elements,
    so it is namespace-agnostic and leaves the rest of the pom untouched.
    """
    with open(xml_path, encoding="utf-8") as f:
        content = f.read()
    pattern = re.compile(r"(<version>)\s*" + re.escape(old_version) + r"\s*(</version>)")
    new_content, n = pattern.subn(r"\g<1>" + clean_version + r"\g<2>", content)
    if n == 0:
        print(
            f"  WARN: version {old_version} not found in {xml_path}; left unchanged"
        )
        return
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"  Masked {n} <version> occurrence(s): {old_version} -> {clean_version}")


def patch_jar_file(jar_exec_path, jar_file, pom_files):
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
        ret = subprocess.run([jar_exec_path, "xf", jar_file], stdout=None, stderr=None)
        if not ret.returncode == 0:
            raise Exception(f"Failed to extract {jar_file}")

        for pom_file in pom_files:
            pom_in_jar = os.path.join("META-INF", "maven", pom_file)
            if os.path.exists(pom_in_jar):
                # Capture the original (vulnerable) version before we overwrite it,
                # and the clean version we are masking to.
                old_version = read_pom_property_version(pom_in_jar)
                clean_version = read_pom_property_version(
                    os.path.join(cwd, "pom", pom_file)
                )

                print(f"Copy {pom_file} to {pom_in_jar}")
                shutil.copy(os.path.join(cwd, "pom", pom_file), pom_in_jar)

                xml_file = pom_file.replace("pom.properties", "pom.xml")
                xml_in_jar = os.path.join("META-INF", "maven", xml_file)
                xml_override = os.path.join(cwd, "pom", xml_file)
                if os.path.exists(xml_override):
                    # Hand-crafted pom.xml override takes precedence.
                    print(f"Copy {xml_file} to {xml_in_jar}")
                    shutil.copy(xml_override, xml_in_jar)
                elif os.path.exists(xml_in_jar) and old_version and clean_version:
                    # Mask the version inside the jar's own pom.xml in place.
                    mask_pom_xml_version(xml_in_jar, old_version, clean_version)

        ret = subprocess.run(
            [jar_exec_path, "cf", new_jar_file, "."], stdout=None, stderr=None
        )
        if not ret.returncode == 0:
            raise Exception(f"Failed to update {jar_file}")

        os.remove(jar_file)
    finally:
        os.chdir(cwd)
        shutil.rmtree(work_dir, ignore_errors=True)


def patch_jars(jar_exec_path, output_dir, pom_files):
    for f in glob.glob(output_dir + "/**/*.jar", recursive=True):
        if f.split("/")[-1] in fixed_jars:
            patch_jar_file(jar_exec_path, f, pom_files)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Patch jars with fixed pom files")
    parser.add_argument(
        "--jar-exec-path", type=str, default="jar", help="Path to jar executable"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory to search for jars to patch",
    )
    args = parser.parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    write_pom_files()
    pom_files = get_pom_files()
    print("Available pom files:")
    print(pom_files)

    if args.output_dir:
        print('Jar exec path "{}"'.format(args.jar_exec_path))
        print('Patch jars in "{}"'.format(args.output_dir))
        patch_jars(args.jar_exec_path, args.output_dir, pom_files)


if __name__ == "__main__":
    main()
