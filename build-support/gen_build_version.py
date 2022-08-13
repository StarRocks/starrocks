#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

import argparse
import os
import subprocess

from datetime import datetime

def func(a, *args, **kwargs):
    print(a)
    print(args)
    print(kwargs)

def get_version():
    version = os.getenv("STARROCKS_VERSION")
    if not version:
        version = "UNKNOWN"
    return version.upper()

def get_commit_hash():
    git_res = subprocess.Popen(["git", "rev-parse", "--short", "HEAD"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = git_res.communicate()

    commit_hash = ''
    if git_res.returncode == 0:
        commit_hash = out.decode('utf-8').strip()
    return commit_hash

def get_build_type():
    build_type = os.getenv("BUILD_TYPE")
    if not build_type:
        build_type = "UNKNOWN"
    return build_type.upper()

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_user():
    user = os.getenv("USER")
    if not user:
        user = "StarRocks"
    return user

def get_hostname():
    if os.path.exists('/.dockerenv'):
        return "docker"
    res = subprocess.Popen(["hostname", "-f"], stdout=subprocess.PIPE)
    out, err = res.communicate()
    return out.decode('utf-8').strip()

def get_java_version():
    java_home = os.getenv("JAVA_HOME")
    java_res = subprocess.Popen([java_home + "/bin/java", "-fullversion"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = java_res.communicate()

    if java_res.returncode == 0:
        return out.decode('utf-8').replace("\"", "\\\"").strip()
    return "unknown jdk"

def skip_write_if_commit_unchanged(file_name, file_content, commit_hash):
    if os.path.exists(file_name):
        with open(file_name) as fh:
            data = fh.read()
            import re
            m = re.search("COMMIT_HASH: (?P<commit_hash>\w+)", data)
            old_commit_hash = m.group('commit_hash') if m else None
            print('gen_build_version.py {}: old commit = {}, new commit = {}'.format(file_name, old_commit_hash, commit_hash))
            if old_commit_hash == commit_hash:
                return
    with open(file_name, 'w') as fh:
        fh.write(file_content)

def generate_java_file(java_path, version, commit_hash, build_type, build_time, user, host, java_version):
    file_format = '''

package com.starrocks.common;

// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
// This is a generated file, DO NOT EDIT IT.
// COMMIT_HASH: {COMMIT_HASH}

public class Version {{
    public static final String STARROCKS_VERSION = "{VERSION}";
    public static final String STARROCKS_COMMIT_HASH = "{COMMIT_HASH}";
    public static final String STARROCKS_BUILD_TYPE = "{BUILD_TYPE}";
    public static final String STARROCKS_BUILD_TIME = "{BUILD_TIME}";
    public static final String STARROCKS_BUILD_USER = "{BUILD_USER}";
    public static final String STARROCKS_BUILD_HOST = "{BUILD_HOST}";
    public static final String STARROCKS_JAVA_COMPILE_VERSION = "{JAVA_VERSION}";
}}
'''
    file_content = file_format.format(VERSION = version, COMMIT_HASH = commit_hash,
            BUILD_TYPE = build_type, BUILD_TIME = build_time, BUILD_USER=user, BUILD_HOST=host,
            JAVA_VERSION = java_version)

    file_name = java_path + "/com/starrocks/common/Version.java"
    d = os.path.dirname(file_name)
    if not os.path.exists(d):
        os.makedirs(d)
    skip_write_if_commit_unchanged(file_name, file_content, commit_hash)

def generate_cpp_file(cpp_path, version, commit_hash, build_type, build_time, user, host):
    file_format = '''
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
// NOTE: This is a generated file, DO NOT EDIT IT
// COMMIT_HASH: {COMMIT_HASH}

namespace starrocks {{

const char* STARROCKS_VERSION = "{VERSION}";
const char* STARROCKS_COMMIT_HASH = "{COMMIT_HASH}";
const char* STARROCKS_BUILD_TYPE = "{BUILD_TYPE}";
const char* STARROCKS_BUILD_TIME = "{BUILD_TIME}";
const char* STARROCKS_BUILD_USER = "{BUILD_USER}";
const char* STARROCKS_BUILD_HOST = "{BUILD_HOST}";
}}

'''
    file_content = file_format.format(VERSION = version, COMMIT_HASH = commit_hash,
            BUILD_TYPE = build_type, BUILD_TIME = build_time,
            BUILD_USER = user, BUILD_HOST = host)

    file_name = cpp_path + "/version.cpp"
    d = os.path.dirname(file_name)
    if not os.path.exists(d):
        os.makedirs(d)
    skip_write_if_commit_unchanged(file_name, file_content, commit_hash)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp", dest='cpp_path', default="./version.cpp", help="Path of generated cpp file", type=str)
    parser.add_argument("--java", dest='java_path', default="./Version.java", help="Path of generated java file", type=str)
    args = parser.parse_args()

    version = get_version()
    commit_hash = get_commit_hash()
    build_type = get_build_type()
    build_time = get_current_time()
    user = get_user()
    hostname = get_hostname()

    java_version = get_java_version()

    generate_cpp_file(args.cpp_path, version, commit_hash, build_type, build_time, user, hostname)
    generate_java_file(args.java_path, version, commit_hash, build_type, build_time, user, hostname, java_version)

if __name__ == '__main__':
    main()
