#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict
import glob
import re
import sys

def gen_combined_notice(licenses_paths, outpath, gen_all):
    all = defaultdict(lambda: [None, None])
    for licenses_path in licenses_paths.split(','):
        notices = glob.glob(licenses_path + '/NOTICE-*.txt')
        for n in notices:
            name = re.search(r'NOTICE-(.+)\.txt', n).group(1)
            all[name][1] = n
        if gen_all:
            licenses = glob.glob(licenses_path + '/LICENSE-*.txt')
            for l in licenses:
                name = re.search(r'LICENSE-(.+)\.txt', l).group(1)
                all[name][0] = l
    with open(outpath, 'wt', encoding='utf-8') as fout:
        fout.write('StarRocks\n\nCopyright 2021-present, StarRocks Inc.\n')
        for name in sorted(all.keys(), key=str.lower):
            e = all[name]
            if e[1]:
                with open(e[1], 'rt', encoding='utf-8') as fin:
                    fout.write('\n---------------------------\n%s NOTICE\n---------------------------\n%s\n' % (name, fin.read()))
            if e[0]:
                with open(e[0], 'rt', encoding='utf-8') as fin:
                    fout.write('\n---------------------------\n%s LICENSE\n---------------------------\n%s\n' % (name, fin.read()))

if __name__ == "__main__":
    gen_combined_notice(sys.argv[1], sys.argv[2], len(sys.argv) >= 4 and sys.argv[3] == 'all')
