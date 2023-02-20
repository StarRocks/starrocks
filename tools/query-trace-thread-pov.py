#!/usr/bin/env python
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

# This tool is to change POV of trace graph, from pipeline driver to thread.
# usage:
#   ./query-trace-thread-pov.py < pipeline-driver.trace > thread.trace  // use pipe
#   ./query-trace-thread-pov.py --input pipeline-driver.trace --output thread.trace // use file
#   ./query-trace-thread-pov.py < pipeline-driver.trace | gzip > thread.trace.gz // use pipe and compress

import json
import sys


def load_events(fh):
    for s in fh:
        s = s.strip()
        if not s: continue
        if s.startswith('{'):
            if s.startswith('{"traceEvents"'): continue
            if s.startswith('{}'): continue
            if s.endswith(','):
                s = s[:-1]
            yield json.loads(s)


def remove_fields(d):
    for f in ('id', 'tidx', 'args'):
        if f in d:
            del d[f]


def fix_events(fh, events, prefilters, postfilters):
    io_tid_set = set()
    exec_tid_set = set()

    fh.write('{"traceEvents":[\n')

    # pid: fragment instance id low bits.
    # tid: pipeline driver pointer.
    def fix_io_task(ev):
        d = ev.copy()
        d.update({'pid': '0', 'tid': d['tidx'], 'name': d['cat']})
        d['ph'] = d['ph'].upper()
        d['cat'] = d['tid']
        # id: chunk source pointer.
        return d

    def fix_driver(ev):
        d = ev.copy()
        d.update({'pid': 0, 'tid': d['tidx'], 'name': d['cat'] + '_' + d['tid']})
        d['cat'] = d['tid']
        return d

    def fix_operator(ev):
        d = ev.copy()
        d.update({'pid': 0, 'tid': d['tidx']})
        return d

    for ev in events:
        name = ev.get('name')
        cat = ev.get('cat')
        d = None
        if any(f(ev) for f in prefilters): continue
        if name == 'io_task':
            d = fix_io_task(ev)
            io_tid_set.add(d['tid'])
        else:
            if name == 'process' and cat.startswith('driver'):
                d = fix_driver(ev)
            elif cat in ('pull_chunk', 'push_chunk'):
                d = fix_operator(ev)
            if d:
                exec_tid_set.add(d['tid'])
        if not d:
            continue
        remove_fields(d)
        if any(f(ev) for f in postfilters): continue
        fh.write(json.dumps(d) + ',\n')

    headers = []
    headers.append(dict(name='process_name', ph='M', pid=0, args={'name': 'starrocks_be'}))
    for tid in io_tid_set:
        d = dict(name='thread_name', ph="M", pid=0, tid=tid, args={"name": "IOThread-%s" % tid})
        headers.append(d)
    for tid in exec_tid_set:
        d = dict(name='thread_name', ph="M", pid=0, tid=tid, args={"name": "ExecThread-%s" % tid})
        headers.append(d)
    for d in headers:
        fh.write(json.dumps(d) + ',\n')
    fh.write('{}]}')


def drill_events(fh, events, text):
    fh.write('{"traceEvents":[\n')
    for ev in events:
        if ev['ph'] == 'M' or ev['name'].find(text) != -1:
            fh.write(json.dumps(ev) + ',\n')
    fh.write('{}]}')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='input file')
    parser.add_argument('--output', help='output file')
    parser.add_argument('-v', action='store_true', help='output detailed version')
    parser.add_argument('-c', action='store_true', help='compress output file')
    parser.add_argument('--drill', help='to drill down a single operator by name')
    args = parser.parse_args()

    fin = sys.stdin
    if args.input:
        fin = open(args.input)
    fout = sys.stdout
    if args.output:
        fout = open(args.output, 'w')

    prefilters = []
    postfilters = []
    if not args.v:
        prefilters.append(lambda x: x.get('cat') in ('push_chunk', 'pull_chunk'))

    events = load_events(fin)

    if args.drill:
        drill_events(fout, events, args.drill)
    else:
        fix_events(fout, events, prefilters, postfilters)


if __name__ == '__main__':
    main()
