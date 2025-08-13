#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import hashlib
import os
import re

def get_license_toggle():
    """Get license toggle value from environment variable"""
    license_toggle = os.getenv("STARROCKS_LICENSE_TOGGLE")
    if not license_toggle:
        return "true"
    # Convert to lowercase and check if it's "true"
    return str(license_toggle.lower() == "true").lower()

def get_fingerprint(items):
    if not isinstance(items, list):
        items = [items]
    return hashlib.md5(",".join(items).encode()).hexdigest()

def skip_write_if_fingerprint_unchanged(file_name, file_content, fingerprint):
    if os.path.exists(file_name):
        with open(file_name) as fh:
            data = fh.read()
            m = re.search(r"FINGERPRINT: (?P<fingerprint>\w+)", data)
            old_fingerprint = m.group('fingerprint') if m else None
            print('gen_license_toggle.py {}: old fingerprint = {}, new fingerprint = {}'.format(file_name, old_fingerprint, fingerprint))
            if old_fingerprint == fingerprint:
                return
    with open(file_name, 'w') as fh:
        fh.write(file_content)

def generate_java_file(java_path, is_enabled):
    file_format = '''
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is a generated file, DO NOT EDIT IT.
// FINGERPRINT: {FINGERPRINT}

package com.starrocks.epack.system;


public class LicenseToggle {{
    public static final boolean isEnabled = {IS_ENABLED};
}}
'''
    fingerprint = get_fingerprint([is_enabled])
    file_content = file_format.format(IS_ENABLED=is_enabled, FINGERPRINT=fingerprint)

    file_name = java_path + "/com/starrocks/epack/system/LicenseToggle.java"
    d = os.path.dirname(file_name)
    if not os.path.exists(d):
        os.makedirs(d)
    skip_write_if_fingerprint_unchanged(file_name, file_content, fingerprint)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", dest='java_path', default="./LicenseToggle.java", help="Path of generated java file", type=str)
    args = parser.parse_args()

    is_enabled = get_license_toggle()
    generate_java_file(args.java_path, is_enabled)

if __name__ == '__main__':
    main() 
