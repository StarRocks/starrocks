// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/url_coding.h"

#include <cmath>
#include <exception>
#include <memory>

namespace starrocks {
static void encode_base64_internal(const std::string& in, std::string* out, const unsigned char* basis, bool padding) {
    size_t len = in.size();
    // Every 3 source bytes will be encoded into 4 bytes.
    std::unique_ptr<unsigned char[]> buf(new unsigned char[(((len + 2) / 3) * 4)]);
    const auto* s = reinterpret_cast<const unsigned char*>(in.data());
    unsigned char* d = buf.get();
    while (len > 2) {
        *d++ = basis[(s[0] >> 2) & 0x3f];
        *d++ = basis[((s[0] & 3) << 4) | (s[1] >> 4)];
        *d++ = basis[((s[1] & 0x0f) << 2) | (s[2] >> 6)];
        *d++ = basis[s[2] & 0x3f];

        s += 3;
        len -= 3;
    }
    if (len) {
        *d++ = basis[(s[0] >> 2) & 0x3f];
        if (len == 1) {
            *d++ = basis[(s[0] & 3) << 4];
            if (padding) {
                *d++ = '=';
            }
        } else {
            *d++ = basis[((s[0] & 3) << 4) | (s[1] >> 4)];
            *d++ = basis[(s[1] & 0x0f) << 2];
        }
        if (padding) {
            *d++ = '=';
        }
    }
    out->assign((char*)buf.get(), d - buf.get());
}

void base64_encode(const std::string& in, std::string* out) {
    static unsigned char basis64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    encode_base64_internal(in, out, basis64, true);
}

static const char base64_pad = '=';

static short decoding_table[256] = {
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -1, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 62, -2, -2, -2, 63, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, -2, -2, -2, -2, -2, -2, -2, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
        13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -2, -2, -2, -2, -2, -2, 26, 27, 28, 29, 30, 31, 32,
        33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2};

static inline int64_t base64_decode(const char* data, size_t length, char* decoded_data) {
    const char* current = data;
    int ch = 0;
    int i = 0;
    int j = 0;
    int k = 0;

    // run through the whole string, converting as we go
    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decoding_table[ch];
        // a space or some other separator character, we simply skip over
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
        case 0:
            decoded_data[j] = ch << 2;
            break;
        case 1:
            decoded_data[j++] |= ch >> 4;
            decoded_data[j] = (ch & 0x0f) << 4;
            break;
        case 2:
            decoded_data[j++] |= ch >> 2;
            decoded_data[j] = (ch & 0x03) << 6;
            break;
        case 3:
            decoded_data[j++] |= ch;
            break;
        default:
            break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
        case 1:
            return 0;
        case 2:
            k++;
        case 3:
            decoded_data[k] = 0;
        default:
            break;
        }
    }

    decoded_data[j] = '\0';

    return j;
}

bool base64_decode(const std::string& in, std::string* out) {
    char* tmp = new char[in.length()];

    int64_t len = base64_decode(in.c_str(), in.length(), tmp);
    if (len < 0) {
        delete[] tmp;
        return false;
    }
    out->assign(tmp, len);
    delete[] tmp;
    return true;
}

} // namespace starrocks
