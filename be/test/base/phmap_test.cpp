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

#include "base/phmap/phmap.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "base/phmap/phmap_dump.h"
#include "base/testutil/parallel_test.h"

namespace starrocks {
class PhmapTest : public testing::Test {};

static bool tls_alloc_succ = true;

template <class T, int Spec = 1>
class CheckedAlloc {
public:
    using value_type = T;

    CheckedAlloc() = default;
    CheckedAlloc(const CheckedAlloc&) = default;
    CheckedAlloc& operator=(const CheckedAlloc&) = default;

    template <class U>
    CheckedAlloc(const CheckedAlloc<U, Spec>& that) {}

    template <class U>
    struct rebind {
        using other = CheckedAlloc<U, Spec>;
    };

    T* allocate(size_t n) {
        if (!tls_alloc_succ) {
            throw std::bad_alloc();
        }
        T* ptr = std::allocator<T>().allocate(n);
        return ptr;
    }
    void deallocate(T* ptr, size_t n) {
        memset(ptr, 0, n * sizeof(T)); // The freed memory must be unpoisoned.
        return std::allocator<T>().deallocate(ptr, n);
    }

    void swap(CheckedAlloc& that) { using std::swap; }
};

PARALLEL_TEST(PhmapTest, resize_fail) {
    using phmap_hash = phmap::priv::hash_default_hash<int32_t>;
    using phmap_equal = phmap::priv::hash_default_eq<int32_t>;
    using phmap_alloc = CheckedAlloc<int32_t>;
    phmap::flat_hash_map<int32_t, int32_t, phmap_hash, phmap_equal, phmap_alloc> map;

    size_t i;
    tls_alloc_succ = true;
    try {
        for (i = 0; i < 1000; i++) {
            if (i == 100) {
                tls_alloc_succ = false;
            }
            map.lazy_emplace(i, [&](const auto& ctor) { ctor(i, i); });
        }
    } catch (std::bad_alloc const&) {
    }

    size_t j = 0;
    auto iter = map.begin();
    auto end = map.end();
    while (iter != end) {
        j++;
        iter++;
    }
    ASSERT_EQ(i, j);
}

struct Value {
public:
    Value(int i) {
        _ptr = new int;
        *_ptr = i;
    }

    ~Value() { delete _ptr; }

private:
    int* _ptr = nullptr;
};

PARALLEL_TEST(PhmapTest, lazy_emplace_fail) {
    using phmap_hash = phmap::priv::hash_default_hash<int32_t>;
    using phmap_equal = phmap::priv::hash_default_eq<int32_t>;
    using phmap_alloc = CheckedAlloc<int32_t>;

    phmap::flat_hash_map<int32_t, uint8_t*, phmap_hash, phmap_equal, phmap_alloc> map;
    size_t i;
    tls_alloc_succ = true;
    try {
        for (i = 0; i < 1000; i++) {
            if (i == 100) {
                tls_alloc_succ = false;
            }
            map.lazy_emplace(i, [&](const auto& ctor) {
                if (!tls_alloc_succ) {
                    throw std::bad_alloc();
                }
                auto* ptr = new Value(i);
                ctor(i, reinterpret_cast<uint8_t*>(ptr));
            });
        }
    } catch (std::bad_alloc const&) {
    }

    size_t j = 0;
    auto iter = map.begin();
    auto end = map.end();
    while (iter != end) {
        j++;
        delete reinterpret_cast<Value*>(iter->second);
        iter++;
    }
    ASSERT_EQ(i, j);
}

PARALLEL_TEST(PhmapTest, iterate) {
    phmap::parallel_flat_hash_map<int32_t, int32_t> map;
    for (int i = 0; i <= 100; i++) {
        map.insert({i, i});
    }
    int sum = 0;
    map.for_each([&](const auto& pair) { sum += pair.second; });
    ASSERT_EQ(sum, 5050);
    map.for_each_m([&](auto& pair) { pair.second++; });
    sum = 0;
    map.for_each([&](const auto& pair) { sum += pair.second; });
    ASSERT_EQ(sum, 5151);

    sum = 0;
    for (int i = 0; i < map.subcnt(); i++) {
        map.with_submap(i, [&](const auto& submap) {
            for (const auto& pair : submap) {
                sum += pair.second;
            }
        });
    }
    ASSERT_EQ(sum, 5151);
    for (int i = 0; i < map.subcnt(); i++) {
        map.with_submap_m(i, [&](auto& submap) {
            for (auto& pair : submap) {
                pair.second--;
            }
        });
    }
    sum = 0;
    for (int i = 0; i < map.subcnt(); i++) {
        map.with_submap(i, [&](const auto& submap) {
            for (const auto& pair : submap) {
                sum += pair.second;
            }
        });
    }
    ASSERT_EQ(sum, 5050);
}

PARALLEL_TEST(PhmapTest, group_matches_scalar_oracle) {
    using phmap::priv::ctrl_t;
    using phmap::priv::Group;
    using phmap::priv::GroupPortableImpl;
    using phmap::priv::h2_t;
    using phmap::priv::kDeleted;
    using phmap::priv::kEmpty;
    using phmap::priv::kSentinel;

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> byte_dist(-3, 127);
    std::uniform_int_distribution<int> special_dist(-3, 7);
    std::uniform_int_distribution<int> h2_dist(0, 127);

    auto decode_ctrl = [](int v) -> ctrl_t {
        if (v == -3) return kEmpty;
        if (v == -2) return kDeleted;
        if (v == -1) return kSentinel;
        return static_cast<ctrl_t>(v);
    };

    auto check_mask = []<typename MaskT>(MaskT bm, int width, const std::vector<int>& expected) {
        std::vector<int> actual;
        for (int idx : bm) {
            actual.push_back(idx);
        }
        ASSERT_EQ(actual, expected);
        ASSERT_EQ(static_cast<bool>(bm), !expected.empty());
        if (!expected.empty()) {
            ASSERT_EQ(bm.TrailingZeros(), expected.front());
            ASSERT_EQ(bm.LeadingZeros(), width - 1 - expected.back());
        }
    };

    auto check_portable_match = [](auto bm, const ctrl_t* ctrl_buf, int width, const std::vector<int>& expected) {
        std::vector<int> actual;
        for (int idx : bm) {
            actual.push_back(idx);
        }
        if (expected.empty()) {
            ASSERT_TRUE(actual.empty());
            ASSERT_FALSE(static_cast<bool>(bm));
            return;
        }
        ASSERT_TRUE(static_cast<bool>(bm));
        for (int idx : expected) {
            ASSERT_NE(std::find(actual.begin(), actual.end(), idx), actual.end());
        }
        for (int idx : actual) {
            ASSERT_GE(idx, 0);
            ASSERT_LT(idx, width);
            ASSERT_GE(ctrl_buf[idx], 0);
        }
        ASSERT_EQ(bm.TrailingZeros(), actual.front());
        ASSERT_EQ(bm.LeadingZeros(), width - 1 - actual.back());
    };

    alignas(16) ctrl_t raw_buf[24];
    for (int iter = 0; iter < 100000; ++iter) {
        std::fill(std::begin(raw_buf), std::end(raw_buf), kEmpty);
        ctrl_t* buf = raw_buf + (iter & 7);
        for (int i = 0; i < Group::kWidth; ++i) {
            int raw = (iter & 1) ? special_dist(rng) : byte_dist(rng);
            buf[i] = decode_ctrl(raw);
        }

        Group g(buf);
        auto h2 = static_cast<h2_t>(h2_dist(rng));

        std::vector<int> expected_match;
        std::vector<int> expected_empty;
        std::vector<int> expected_empty_or_deleted;
        for (int i = 0; i < Group::kWidth; ++i) {
            if (buf[i] == static_cast<ctrl_t>(h2)) {
                expected_match.push_back(i);
            }
            if (buf[i] == kEmpty) {
                expected_empty.push_back(i);
            }
            if (buf[i] == kEmpty || buf[i] == kDeleted) {
                expected_empty_or_deleted.push_back(i);
            }
        }

        if constexpr (!std::is_same_v<Group, GroupPortableImpl>) {
            check_mask(g.Match(h2), Group::kWidth, expected_match);
        } else {
            check_portable_match(g.Match(h2), buf, Group::kWidth, expected_match);
        }
        check_mask(g.MatchEmpty(), Group::kWidth, expected_empty);
        check_mask(g.MatchEmptyOrDeleted(), Group::kWidth, expected_empty_or_deleted);

        uint32_t expected_leading = 0;
        while (expected_leading < static_cast<uint32_t>(Group::kWidth) &&
               (buf[expected_leading] == kEmpty || buf[expected_leading] == kDeleted)) {
            ++expected_leading;
        }
        ASSERT_EQ(g.CountLeadingEmptyOrDeleted(), expected_leading);

        // Also exercise GroupPortableImpl directly at unaligned offsets on all platforms.
        GroupPortableImpl gp(buf);
        std::vector<int> expected_portable_match;
        std::vector<int> expected_portable_empty;
        std::vector<int> expected_portable_empty_or_deleted;
        for (int i = 0; i < GroupPortableImpl::kWidth; ++i) {
            if (buf[i] == static_cast<ctrl_t>(h2)) {
                expected_portable_match.push_back(i);
            }
            if (buf[i] == kEmpty) {
                expected_portable_empty.push_back(i);
            }
            if (buf[i] == kEmpty || buf[i] == kDeleted) {
                expected_portable_empty_or_deleted.push_back(i);
            }
        }
        check_portable_match(gp.Match(h2), buf, GroupPortableImpl::kWidth, expected_portable_match);
        check_mask(gp.MatchEmpty(), GroupPortableImpl::kWidth, expected_portable_empty);
        check_mask(gp.MatchEmptyOrDeleted(), GroupPortableImpl::kWidth, expected_portable_empty_or_deleted);
        uint32_t expected_portable_leading = 0;
        while (expected_portable_leading < static_cast<uint32_t>(GroupPortableImpl::kWidth) &&
               (buf[expected_portable_leading] == kEmpty || buf[expected_portable_leading] == kDeleted)) {
            ++expected_portable_leading;
        }
        ASSERT_EQ(gp.CountLeadingEmptyOrDeleted(), expected_portable_leading);
    }
}

PARALLEL_TEST(PhmapTest, group_adjacent_h2_exact) {
    using phmap::priv::ctrl_t;
    using phmap::priv::Group;
    using phmap::priv::GroupPortableImpl;
    using phmap::priv::kEmpty;

    alignas(16) ctrl_t buf[16];
    std::fill(std::begin(buf), std::end(buf), kEmpty);
    buf[2] = static_cast<ctrl_t>(0x12);
    buf[3] = static_cast<ctrl_t>(0x13);

    Group g(buf);

    std::vector<int> m12;
    for (int idx : g.Match(0x12)) {
        m12.push_back(idx);
    }
    if constexpr (!std::is_same_v<Group, GroupPortableImpl>) {
        ASSERT_EQ(m12, std::vector<int>{2});
    } else {
        ASSERT_EQ(m12, (std::vector<int>{2, 3}));
    }

    std::vector<int> m13;
    for (int idx : g.Match(0x13)) {
        m13.push_back(idx);
    }
    ASSERT_EQ(m13, std::vector<int>{3});

    GroupPortableImpl gp(buf);
    std::vector<int> mp12;
    for (int idx : gp.Match(0x12)) {
        mp12.push_back(idx);
    }
    ASSERT_EQ(mp12, (std::vector<int>{2, 3}));
}

PARALLEL_TEST(PhmapTest, group_all_empty_or_deleted) {
    using phmap::priv::ctrl_t;
    using phmap::priv::Group;
    using phmap::priv::GroupPortableImpl;
    using phmap::priv::kDeleted;
    using phmap::priv::kEmpty;

    alignas(16) ctrl_t buf[24];
    for (size_t off = 0; off < 8; ++off) {
        std::fill(std::begin(buf), std::end(buf), kEmpty);
        ASSERT_EQ(Group(buf + off).CountLeadingEmptyOrDeleted(), static_cast<uint32_t>(Group::kWidth));
        ASSERT_EQ(GroupPortableImpl(buf + off).CountLeadingEmptyOrDeleted(),
                  static_cast<uint32_t>(GroupPortableImpl::kWidth));

        std::fill(std::begin(buf), std::end(buf), kDeleted);
        ASSERT_EQ(Group(buf + off).CountLeadingEmptyOrDeleted(), static_cast<uint32_t>(Group::kWidth));
        ASSERT_EQ(GroupPortableImpl(buf + off).CountLeadingEmptyOrDeleted(),
                  static_cast<uint32_t>(GroupPortableImpl::kWidth));
    }
}

PARALLEL_TEST(PhmapTest, group_convert_special) {
    using phmap::priv::ctrl_t;
    using phmap::priv::Group;
    using phmap::priv::GroupPortableImpl;
    using phmap::priv::kDeleted;
    using phmap::priv::kEmpty;
    using phmap::priv::kSentinel;

    const ctrl_t pattern[16] = {
            kEmpty, kDeleted, kSentinel, 0,      1,      static_cast<ctrl_t>(0x7F),
            kEmpty, 0x2A,     kEmpty,    kEmpty, kEmpty, kEmpty,
            kEmpty, kEmpty,   kEmpty,    kEmpty,
    };

    for (size_t off = 0; off < 8; ++off) {
        alignas(16) ctrl_t in_buf[24];
        alignas(16) ctrl_t out_buf[24];
        std::fill(std::begin(in_buf), std::end(in_buf), kEmpty);
        std::fill(std::begin(out_buf), std::end(out_buf), static_cast<ctrl_t>(0));
        std::copy(std::begin(pattern), std::end(pattern), in_buf + off);

        Group(in_buf + off).ConvertSpecialToEmptyAndFullToDeleted(out_buf + off);
        for (int i = 0; i < Group::kWidth; ++i) {
            if (pattern[i] < 0) {
                ASSERT_EQ(out_buf[off + i], kEmpty) << "off=" << off << " index=" << i;
            } else {
                ASSERT_EQ(out_buf[off + i], kDeleted) << "off=" << off << " index=" << i;
            }
        }

        std::fill(std::begin(out_buf), std::end(out_buf), static_cast<ctrl_t>(0));
        GroupPortableImpl(in_buf + off).ConvertSpecialToEmptyAndFullToDeleted(out_buf + off);
        for (int i = 0; i < GroupPortableImpl::kWidth; ++i) {
            if (pattern[i] < 0) {
                ASSERT_EQ(out_buf[off + i], kEmpty) << "off=" << off << " index=" << i;
            } else {
                ASSERT_EQ(out_buf[off + i], kDeleted) << "off=" << off << " index=" << i;
            }
        }
    }
}

PARALLEL_TEST(PhmapTest, erase_and_rehash) {
    auto run_test = [](auto& map) {
        for (int64_t k = 0; k < 10000; ++k) {
            map[k] = k * 3 + 1;
        }
        ASSERT_EQ(map.size(), 10000U);

        for (int64_t k = 0; k < 10000; k += 2) {
            ASSERT_EQ(map.erase(k), 1U);
        }
        ASSERT_EQ(map.size(), 5000U);

        for (int64_t k = 10000; k < 15000; ++k) {
            map[k] = k * 5 - 7;
        }
        ASSERT_EQ(map.size(), 10000U);

        for (int64_t k = 0; k < 10000; ++k) {
            auto it = map.find(k);
            if (k % 2 == 0) {
                ASSERT_EQ(it, map.end());
            } else {
                ASSERT_NE(it, map.end());
                ASSERT_EQ(it->second, k * 3 + 1);
            }
        }
        for (int64_t k = 10000; k < 15000; ++k) {
            auto it = map.find(k);
            ASSERT_NE(it, map.end());
            ASSERT_EQ(it->second, k * 5 - 7);
        }
    };

    phmap::flat_hash_map<int64_t, int64_t> flat_map;
    run_test(flat_map);

    phmap::parallel_flat_hash_map<int64_t, int64_t> parallel_map;
    run_test(parallel_map);
}

PARALLEL_TEST(PhmapTest, dump_load_roundtrip) {
    phmap::flat_hash_map<int32_t, int32_t> src;
    for (int32_t i = 0; i < 200; ++i) {
        src[i * 7 + 3] = i * 11 - 5;
    }
    ASSERT_EQ(src.size(), 200U);

    std::string tmpl = (std::filesystem::temp_directory_path() / "phmap_dump_test_XXXXXX").string();
    int fd = ::mkstemp(tmpl.data());
    ASSERT_GE(fd, 0);
    ::close(fd);
    struct TempFileGuard {
        std::string path;
        ~TempFileGuard() {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    } guard{tmpl};

    {
        phmap::BinaryOutputArchive out_ar(tmpl.c_str());
        ASSERT_TRUE(src.dump(out_ar).ok());
        ASSERT_TRUE(out_ar.close());
    }

    phmap::flat_hash_map<int32_t, int32_t> dst;
    {
        phmap::BinaryInputArchive in_ar(tmpl.c_str());
        ASSERT_TRUE(dst.load(in_ar).ok());
    }

    ASSERT_EQ(dst.size(), src.size());
    for (const auto& [k, v] : src) {
        auto it = dst.find(k);
        ASSERT_NE(it, dst.end());
        ASSERT_EQ(it->second, v);
    }
}

} // namespace starrocks
