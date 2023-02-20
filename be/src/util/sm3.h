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

#pragma once

#include <memory.h>

#include <cstdio>

namespace starrocks {
// SM3 Cryptographic Hash Algorithm is define by http://www.oscca.gov.cn/sca/xxgk/2010-12/17/content_1002389.shtml .
// and http://www.oscca.gov.cn/sca/xxgk/2010-12/17/1002389/files/302a3ada057c4a73830536d03e683110.pdf.
// Our implementation is based on https://blog.csdn.net/a344288106/article/details/80094878 .
class Sm3 {
    static constexpr int ENDIAN_TEST_VALUE = 1;
    static bool is_little_endian;
    static constexpr int WRAP_AROUND_THRESHOLD = 55;
    static constexpr int WORD_BITS = 32;
    static constexpr int SM3_HASH_SIZE = 32;

    struct Sm3Context {
        // 256 bits as output of sm3 hash algorithm.
        unsigned int intermediate_hash[SM3_HASH_SIZE / 4];
        // 512 bits, used to accept block bits of input.
        unsigned char message_block[64];
    };

    // every parameter is the unsigned version because we use logical shift.
    static unsigned int left_rotate(unsigned int word, int bits);
    static unsigned int* reverse_word(unsigned int* word);
    static unsigned long* reverse_double_word(unsigned long* word);
    static unsigned int T(int i);
    static unsigned int FF(unsigned int X, unsigned int Y, unsigned int Z, int i);
    static unsigned int GG(unsigned int X, unsigned int Y, unsigned int Z, int i);
    static unsigned int P0(unsigned int X);
    static unsigned int P1(unsigned int X);
    static void init(Sm3Context* context);
    static void process_message_block(Sm3Context* context);

public:
    static unsigned char* sm3_compute(const unsigned char* message, unsigned long messageLen,
                                      unsigned char digest[SM3_HASH_SIZE]);
};

} // namespace starrocks
