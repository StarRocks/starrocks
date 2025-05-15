#pragma once

#include <cstdint>

namespace starrocks {
enum PhmapSeed { PhmapSeed1, PhmapSeed2 };

struct CRC_HASH_SEEDS {
    // TODO: 0x811C9DC5 is not prime number
    static const uint32_t CRC_HASH_SEED1 = 0x811C9DC5;
    static const uint32_t CRC_HASH_SEED2 = 0x811C9DD7;
};

template <class T, PhmapSeed seed>
class StdHashWithSeed;

template <PhmapSeed seed>
struct Hash128WithSeed;

template <PhmapSeed seed>
struct Hash256WithSeed;

template <PhmapSeed>
class SliceHashWithSeed;

template <class T>
class StdHash;
} // namespace starrocks