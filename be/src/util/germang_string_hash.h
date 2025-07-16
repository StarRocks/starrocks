#pragma once

#include "column/german_string.h"
#include "util/hash.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {
template <PhmapSeed seed>
static constexpr uint64_t ChooseSeed = CRC_HASH_SEEDS::CRC_HASH_SEED1;
template <>
static constexpr uint64_t ChooseSeed<PhmapSeed2> = CRC_HASH_SEEDS::CRC_HASH_SEED2;

template <PhmapSeed>
class GermanStringHashWithSeed {
public:
    std::size_t operator()(const GermanString& s) const {
        constexpr auto seed = ChooseSeed<PhmapSeed>;
        if (s.is_inline()) {
            return crc_hash_64(s.short_rep.str, static_cast<int32_t>(s.len), seed);
        } else {
            auto seed = crc_hash_64(s.long_rep.prefix, GermanString::PREFIX_LENGTH, seed);
            return crc_hash_64(reinterpret_cast<const char*>(s.long_rep.ptr),
                               static_cast<int32_t>(s.len - GermanString::PREFIX_LENGTH), seed);
        }
    }
};

using GermanStringHash = GermanStringHashWithSeed<PhmapSeed1>;

class GermanStringEqual {
public:
    bool operator()(const GermanString& x, const GermanString& y) const { return x == y; }
};

} // namespace starrocks