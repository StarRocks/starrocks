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

// This file is based on code available under the Apache license here:
//   https://github.com/greg7mdp/parallel-hashmap/blob/master/parallel_hashmap/phmap_dump.h

#pragma once

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
//       providing dump/load/mmap_load
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
// ---------------------------------------------------------------------------

#include <fstream>
#include <iostream>
#include <sstream>
#include <type_traits>

#include "phmap.h"
namespace phmap {

namespace type_traits_internal {

#if defined(__GLIBCXX__) && __GLIBCXX__ < 20150801
template <typename T>
struct IsTriviallyCopyable : public std::integral_constant<bool, __has_trivial_copy(T)> {};
#else
template <typename T>
struct IsTriviallyCopyable : public std::is_trivially_copyable<T> {};
#endif

template <class T1, class T2>
struct IsTriviallyCopyable<std::pair<T1, T2>> {
    static constexpr bool value = IsTriviallyCopyable<T1>::value && IsTriviallyCopyable<T2>::value;
};
} // namespace type_traits_internal

namespace priv {

static constexpr uint32_t PVERSION = 1;

// ------------------------------------------------------------------------
// dump/load for raw_hash_set
// ------------------------------------------------------------------------

template <class Policy, class Hash, class Eq, class Alloc>
size_t raw_hash_set<Policy, Hash, Eq, Alloc>::dump_bound() const {
    if (empty()) {
        // Set empty hash set serialize_size larger than sizeof(uint64_t),
        // In order to improve count distinct streaming aggregate performance
        return sizeof(decltype(size())) * 3;
    } else {
        return sizeof(size_) + sizeof(capacity_) + sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1) +
               sizeof(slot_type) * capacity_;
    }
}

template <class Policy, class Hash, class Eq, class Alloc>
template <typename OutputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::dump(OutputArchive& ar) const {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");

    if (!ar.dump(size_)) {
        std::cerr << "Failed to dump size_" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.dump(capacity_)) {
        std::cerr << "Failed to dump capacity_" << std::endl;
        return false;
    }
    SanitizerUnpoisonMemoryRegion(ctrl_, sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1));
    if (!ar.dump(reinterpret_cast<char*>(ctrl_), sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1))) {
        std::cerr << "Failed to dump ctrl_" << std::endl;
        return false;
    }
    SanitizerUnpoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
    if (!ar.dump(reinterpret_cast<char*>(slots_), sizeof(slot_type) * capacity_)) {
        std::cerr << "Failed to dump slot_" << std::endl;
        return false;
    }
    return true;
}

template <class Policy, class Hash, class Eq, class Alloc>
template <typename InputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");
    raw_hash_set<Policy, Hash, Eq, Alloc>().swap(*this); // clear any existing content
    if (!ar.load(&size_)) {
        std::cerr << "Failed to load size_" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.load(&capacity_)) {
        std::cerr << "Failed to load capacity_" << std::endl;
        return false;
    }

    // allocate memory for ctrl_ and slots_
    initialize_slots();
    SanitizerUnpoisonMemoryRegion(ctrl_, sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1));
    if (!ar.load(reinterpret_cast<char*>(ctrl_), sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1))) {
        std::cerr << "Failed to load ctrl" << std::endl;
        return false;
    }
    SanitizerUnpoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
    if (!ar.load(reinterpret_cast<char*>(slots_), sizeof(slot_type) * capacity_)) {
        std::cerr << "Failed to load slot" << std::endl;
        return false;
    }
    return true;
}

// ------------------------------------------------------------------------
// dump/load for parallel_hash_set
// ------------------------------------------------------------------------
template <size_t N, template <class, class, class, class> class RefSet, class Mtx_, class Policy, class Hash, class Eq,
          class Alloc, bool balance>
size_t parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc, balance>::dump_bound() const {
    size_t ret = sizeof(PVERSION);
    if constexpr (PVERSION > 1) {
        ret += sizeof(decltype(subcnt()));
    }
    for (const auto& inner : sets_) {
        ret += inner.set_.dump_bound();
    }
    return ret;
}

template <size_t N, template <class, class, class, class> class RefSet, class Mtx_, class Policy, class Hash, class Eq,
          class Alloc, bool balance>
template <typename OutputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc, balance>::dump(OutputArchive& ar) const {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");

    if (!ar.dump(PVERSION)) {
        std::cerr << "Failed to dump meta!" << std::endl;
        return false;
    }
    if constexpr (PVERSION > 1) {
        if (!ar.dump(subcnt())) {
            std::cerr << "Failed to dump meta!" << std::endl;
            return false;
        }
    }
    for (size_t i = 0; i < sets_.size(); ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.dump(ar)) {
            std::cerr << "Failed to dump submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

template <size_t N, template <class, class, class, class> class RefSet, class Mtx_, class Policy, class Hash, class Eq,
          class Alloc, bool balance>
template <typename InputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc, balance>::load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                  "value_type should be trivially copyable");

    uint32_t version = 0;
    if (!ar.load(&version)) {
        std::cerr << "Failed to load submap count!" << std::endl;
        return false;
    }
    if (version == 2) {
        size_t submap_count = 0;
        if (!ar.load(&submap_count)) {
            std::cerr << "Failed to load submap count!" << std::endl;
            return false;
        }

        if (submap_count != subcnt()) {
            std::cerr << "submap count(" << submap_count << ") != N(" << N << ")" << std::endl;
            return false;
        }
    } else if (version == 1) {
        // no bubmap count, nothing to do
    } else {
        std::cerr << "invalid version(" << version << ")" << std::endl;
        return false;
    }

    for (size_t i = 0; i < subcnt(); ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.load(ar)) {
            std::cerr << "Failed to load submap " << i << std::endl;
            return false;
        }
    }
    return true;
}
} // namespace priv

template <typename T, T V>
using IntegralConstant = std::integral_constant<T, V>;

template <typename HashSet>
struct item_serialize_size
        : public IntegralConstant<size_t, sizeof(typename HashSet::slot_type) + sizeof(phmap::priv::ctrl_t)> {};

// ------------------------------------------------------------------------
// BinaryArchive
//       File is closed when archive object is destroyed
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------
class BinaryOutputArchive {
public:
    BinaryOutputArchive(const char* file_path) { ofs_.open(file_path, std::ios_base::binary); }

    bool dump(const char* p, size_t sz) {
        ofs_.write(p, sz);
        return true;
    }

    template <typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type dump(const V& v) {
        ofs_.write(reinterpret_cast<const char*>(&v), sizeof(V));
        return true;
    }

private:
    std::ofstream ofs_;
};

class BinaryInputArchive {
public:
    explicit BinaryInputArchive(const char* file_path) { ifs_.open(file_path, std::ios_base::binary); }

    bool load(char* p, size_t sz) {
        ifs_.read(p, sz);
        return true;
    }

    template <typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type load(V* v) {
        ifs_.read(reinterpret_cast<char*>(v), sizeof(V));
        return true;
    }

private:
    std::ifstream ifs_;
};

// Dump hash table content to another continue memory
class InMemoryOutput {
public:
    explicit InMemoryOutput(char* dst) : _start(dst), _dst(dst) {}

    bool dump(const char* p, size_t size) {
        memcpy(_dst, p, size);
        _dst += size;
        return true;
    }

    template <typename V>
    typename std::enable_if<std::is_trivially_copyable<V>::value, bool>::type dump(const V& v) {
        memcpy(_dst, &v, sizeof(V));
        _dst += sizeof(V);
        return true;
    }

    size_t length() const { return _dst - _start; }

private:
    char* _start;
    char* _dst;
};

// Load hash table content from another continue memory
class InMemoryInput {
public:
    explicit InMemoryInput(const char* src) : _src(src) {}

    bool load(char* p, size_t size) {
        memcpy(p, _src, size);
        _src += size;
        return true;
    }

    template <typename V>
    typename std::enable_if<std::is_trivially_copyable<V>::value, bool>::type load(V* v) {
        memcpy(v, _src, sizeof(V));
        _src += sizeof(V);
        return true;
    }

private:
    const char* _src;
};

} // namespace phmap
