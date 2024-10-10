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

#include <cstdint>

//// TODO, up to now, Bit Copy is only used in test and bench for preparing the
//// raw data, so we don't check the performance.
struct BitCopy {
    // Returns at least 'numBits' bits of data starting at bit 'bitOffset'
    // from 'source'. T must be at least 'numBits' wide. If 'numBits' bits
    // from 'bitIffset' do not in T, loads the next byte to get the extra
    // bits.
    template <typename T>
    static inline T loadBits(const uint64_t* source, uint64_t bitOffset, uint8_t numBits) {
        constexpr int32_t kBitSize = 8 * sizeof(T);
        auto address = reinterpret_cast<uint64_t>(source) + bitOffset / 8;
        T word = *reinterpret_cast<const T*>(address);
        auto bit = bitOffset & 7;
        if (!bit) {
            return word;
        }
        if (numBits + bit <= kBitSize) {
            return word >> bit;
        }
        uint8_t lastByte = reinterpret_cast<const uint8_t*>(address)[sizeof(T)];
        uint64_t lastBits = static_cast<T>(lastByte) << (kBitSize - bit);
        return (word >> bit) | lastBits;
    }

    // Stores the 'numBits' low bits of 'word' into bits starting at the
    // 'bitOffset'th bit from target. T must be at least 'numBits'
    // wide. If the bit field that is stored overflows a word of T, writes
    // the trailing bits in the low bits of the next byte. Preserves all
    // bits below and above the written bits.
    template <typename T>
    static inline void storeBits(uint64_t* target, uint64_t offset, uint64_t word, uint8_t numBits) {
        constexpr int32_t kBitSize = 8 * sizeof(T);
        T* address = reinterpret_cast<T*>(reinterpret_cast<uint64_t>(target) + (offset / 8));
        auto bitOffset = offset & 7;
        uint64_t mask = (numBits == 64 ? ~0UL : ((1UL << numBits) - 1)) << bitOffset;
        *address = (*address & ~mask) | (mask & (word << bitOffset));
        if (numBits + bitOffset > kBitSize) {
            uint8_t* lastByteAddress = reinterpret_cast<uint8_t*>(address) + sizeof(T);
            uint8_t lastByteBits = bitOffset + numBits - kBitSize;
            uint8_t lastByteMask = (1 << lastByteBits) - 1;
            *lastByteAddress = (*lastByteAddress & ~lastByteMask) | (lastByteMask & (word >> (kBitSize - bitOffset)));
        }
    }

    // Copies a string of bits between locations in memory given by an
    // address and a bit offset for source and destination.
    static inline void copyBits(const uint64_t* source, uint64_t sourceOffset, uint64_t* target, uint64_t targetOffset,
                                uint64_t numBits) {
        uint64_t i = 0;
        for (; i + 64 <= numBits; i += 64) {
            uint64_t word = loadBits<uint64_t>(source, i + sourceOffset, 64);
            storeBits<uint64_t>(target, targetOffset + i, word, 64);
        }
        if (i + 32 <= numBits) {
            auto lastWord = loadBits<uint32_t>(source, sourceOffset + i, 32);
            storeBits<uint32_t>(target, targetOffset + i, lastWord, 32);
            i += 32;
        }
        if (i + 16 <= numBits) {
            auto lastWord = loadBits<uint16_t>(source, sourceOffset + i, 16);
            storeBits<uint16_t>(target, targetOffset + i, lastWord, 16);
            i += 16;
        }
        for (; i < numBits; i += 8) {
            auto copyBits = std::min<uint64_t>(numBits - i, 8);
            auto lastWord = loadBits<uint8_t>(source, sourceOffset + i, copyBits);
            storeBits<uint8_t>(target, targetOffset + i, lastWord, copyBits);
        }
    }
};
