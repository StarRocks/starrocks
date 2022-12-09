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
#
#!/usr/bin/env python
# coding:utf-8
# Copyright (C) dirlt

from io import StringIO

fbs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
       12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
       23, 24, 26, 28, 30, 32, 40, 48, 56, 64]


class MyStringIO:
    def __init__(self):
        self.io = StringIO()

    def emit(self, x):
        self.io.write((x.decode('utf-8') if hasattr(x, "decode") else x) + '\n')

    def get(self):
        return self.io.getvalue()


def gen_unpack_bit(fb):
    oss = MyStringIO()
    oss.emit('const uint8_t* _unpack64_{fb}(const uint8_t *__restrict__ in, int64_t *__restrict__ out)'.format(
        fb=fb))
    oss.emit('{')
    oss.emit('int64_t t = 0;')
    oss.emit('uint8_t c = 0;')
    # how many bits in cb
    cb = 0

    for i in range(64):
        oss.emit('t = 0;')
        exp = fb
        while exp:
            if cb == 0:
                oss.emit('c = (*in++);')
                cb = 8

            lb = min(exp, cb)
            oss.emit('t = (t << {lb}) | ((c >> {x}) & ((1 << {lb}) - 1));'.format(lb=lb, x=cb - lb))
            exp -= lb
            cb -= lb

        oss.emit('*out = t;')
        oss.emit('out++;')
    oss.emit('return in;')
    oss.emit('}')
    return oss.get()


def gen_unpack_driver():
    oss = MyStringIO()
    for fb in fbs:
        oss.emit('void bit_unpack64_{fb}(const uint8_t* __restrict in, int64_t* __restrict__ out, int nums)'.format(
            fb=fb))
        oss.emit("""{{
        int run = nums / 64;
        for(int i=0;i<run;i++) {{
        in = _unpack64_{fb}(in, out);
        out += 64;
        }}
        bit_unpack_tail(in, {fb}, out, nums % 64);
        }}""".format(fb=fb))

    oss.emit('void bit_unpack(const uint8_t* in, int fb, int64_t* data, int nums) {')
    oss.emit('switch (fb) {')
    for fb in fbs:
        oss.emit('case {fb}:'.format(fb=fb))
        oss.emit('return bit_unpack64_{fb}(in, data, nums);'.format(fb=fb))
    oss.emit('}')
    oss.emit('}')
    return oss.get()


with open('bit_packing_gen.inc', 'w') as fh:
    for fb in fbs:
        s = gen_unpack_bit(fb)
        fh.write(s)
    s = gen_unpack_driver()
    fh.write(s)

if __name__ == '__main__':
    pass
