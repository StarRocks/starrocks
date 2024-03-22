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
//   https://github.com/greg7mdp/parallel-hashmap/blob/master/parallel_hashmap/phmap.h

#if !defined(phmap_h_guard_)
#define phmap_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
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
//
// Includes work from abseil-cpp (https://github.com/abseil/abseil-cpp)
// with modifications.
//
// Copyright 2018 The Abseil Authors.
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

#ifdef _MSC_VER
#pragma warning(push)

#pragma warning(disable : 4127) // conditional expression is constant
#pragma warning(disable : 4324) // structure was padded due to alignment specifier
#pragma warning(disable : 4514) // unreferenced inline function has been removed
#pragma warning(disable : 4623) // default constructor was implicitly defined as deleted
#pragma warning(disable : 4625) // copy constructor was implicitly defined as deleted
#pragma warning(disable : 4626) // assignment operator was implicitly defined as deleted
#pragma warning(disable : 4710) // function not inlined
#pragma warning(disable : 4711) // selected for automatic inline expansion
#pragma warning(disable : 4820) // '6' bytes padding added after data member
#pragma warning(disable : 4868) // compiler may not enforce left-to-right evaluation order in braced initializer list
#pragma warning(disable : 5027) // move assignment operator was implicitly defined as deleted
#pragma warning(disable : 5045) // Compiler will insert Spectre mitigation for memory load if /Qspectre switch specified
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "phmap_base.h"
#include "phmap_fwd_decl.h"
#include "phmap_hash.h"
#include "phmap_utils.h"

#if PHMAP_HAVE_STD_STRING_VIEW
#include <string_view>
#endif

namespace phmap {

namespace priv {

// --------------------------------------------------------------------------
template <size_t Width>
class probe_seq {
public:
    probe_seq(size_t hashval, size_t mask) {
        assert(((mask + 1) & mask) == 0 && "not a mask");
        mask_ = mask;
        offset_ = hashval & mask_;
    }
    size_t offset() const { return offset_; }
    size_t offset(size_t i) const { return (offset_ + i) & mask_; }

    void next() {
        index_ += Width;
        offset_ += index_;
        offset_ &= mask_;
    }
    // 0-based probe index. The i-th probe in the probe sequence.
    size_t getindex() const { return index_; }

private:
    size_t mask_;
    size_t offset_;
    size_t index_ = 0;
};

// --------------------------------------------------------------------------
template <class ContainerKey, class Hash, class Eq>
struct RequireUsableKey {
    template <class PassedKey, class... Args>
    std::pair<decltype(std::declval<const Hash&>()(std::declval<const PassedKey&>())),
              decltype(std::declval<const Eq&>()(std::declval<const ContainerKey&>(),
                                                 std::declval<const PassedKey&>()))>*
    operator()(const PassedKey&, const Args&...) const;
};

// --------------------------------------------------------------------------
template <class E, class Policy, class Hash, class Eq, class... Ts>
struct IsDecomposable : std::false_type {};

template <class Policy, class Hash, class Eq, class... Ts>
struct IsDecomposable<phmap::void_t<decltype(Policy::apply(RequireUsableKey<typename Policy::key_type, Hash, Eq>(),
                                                           std::declval<Ts>()...))>,
                      Policy, Hash, Eq, Ts...> : std::true_type {};

// TODO(alkis): Switch to std::is_nothrow_swappable when gcc/clang supports it.
// --------------------------------------------------------------------------
template <class T>
constexpr bool IsNoThrowSwappable() {
    using std::swap;
    return noexcept(swap(std::declval<T&>(), std::declval<T&>()));
}

// --------------------------------------------------------------------------
template <typename T>
int TrailingZeros(T x) {
    PHMAP_IF_CONSTEXPR(sizeof(T) == 8)
    return base_internal::CountTrailingZerosNonZero64(static_cast<uint64_t>(x));
    else return base_internal::CountTrailingZerosNonZero32(static_cast<uint32_t>(x));
}

// --------------------------------------------------------------------------
template <typename T>
int LeadingZeros(T x) {
    PHMAP_IF_CONSTEXPR(sizeof(T) == 8)
    return base_internal::CountLeadingZeros64(static_cast<uint64_t>(x));
    else return base_internal::CountLeadingZeros32(static_cast<uint32_t>(x));
}

// --------------------------------------------------------------------------
// An abstraction over a bitmask. It provides an easy way to iterate through the
// indexes of the set bits of a bitmask.  When Shift=0 (platforms with SSE),
// this is a true bitmask.  On non-SSE, platforms the arithematic used to
// emulate the SSE behavior works in bytes (Shift=3) and leaves each bytes as
// either 0x00 or 0x80.
//
// For example:
//   for (int i : BitMask<uint32_t, 16>(0x5)) -> yields 0, 2
//   for (int i : BitMask<uint64_t, 8, 3>(0x0000000080800000)) -> yields 2, 3
// --------------------------------------------------------------------------
template <class T, int SignificantBits, int Shift = 0>
class BitMask {
    static_assert(std::is_unsigned<T>::value);
    static_assert(Shift == 0 || Shift == 3);

public:
    // These are useful for unit tests (gunit).
    using value_type = int;
    using iterator = BitMask;
    using const_iterator = BitMask;

    explicit BitMask(T mask) : mask_(mask) {}
    BitMask& operator++() {
        mask_ &= (mask_ - 1);
        return *this;
    }
    explicit operator bool() const { return mask_ != 0; }
    int operator*() const { return LowestBitSet(); }
    int LowestBitSet() const { return priv::TrailingZeros(mask_) >> Shift; }
    int HighestBitSet() const { return (sizeof(T) * CHAR_BIT - priv::LeadingZeros(mask_) - 1) >> Shift; }

    BitMask begin() const { return *this; }
    BitMask end() const { return BitMask(0); }

    int TrailingZeros() const { return priv::TrailingZeros(mask_) >> Shift; }

    int LeadingZeros() const {
        constexpr int total_significant_bits = SignificantBits << Shift;
        constexpr int extra_bits = sizeof(T) * 8 - total_significant_bits;
        return priv::LeadingZeros(mask_ << extra_bits) >> Shift;
    }

private:
    friend bool operator==(const BitMask& a, const BitMask& b) { return a.mask_ == b.mask_; }
    friend bool operator!=(const BitMask& a, const BitMask& b) { return a.mask_ != b.mask_; }

    T mask_;
};

// --------------------------------------------------------------------------
using ctrl_t = signed char;
using h2_t = uint8_t;

// --------------------------------------------------------------------------
// The values here are selected for maximum performance. See the static asserts
// below for details.
// --------------------------------------------------------------------------
enum Ctrl : ctrl_t {
    kEmpty = -128,  // 0b10000000
    kDeleted = -2,  // 0b11111110
    kSentinel = -1, // 0b11111111
};

static_assert(kEmpty & kDeleted & kSentinel & 0x80,
              "Special markers need to have the MSB to make checking for them efficient");
static_assert(kEmpty < kSentinel && kDeleted < kSentinel,
              "kEmpty and kDeleted must be smaller than kSentinel to make the "
              "SIMD test of IsEmptyOrDeleted() efficient");
static_assert(kSentinel == -1,
              "kSentinel must be -1 to elide loading it from memory into SIMD "
              "registers (pcmpeqd xmm, xmm)");
static_assert(kEmpty == -128,
              "kEmpty must be -128 to make the SIMD check for its "
              "existence efficient (psignb xmm, xmm)");
static_assert(~kEmpty & ~kDeleted & kSentinel & 0x7F,
              "kEmpty and kDeleted must share an unset bit that is not shared "
              "by kSentinel to make the scalar test for MatchEmptyOrDeleted() "
              "efficient");
static_assert(kDeleted == -2,
              "kDeleted must be -2 to make the implementation of "
              "ConvertSpecialToEmptyAndFullToDeleted efficient");

// --------------------------------------------------------------------------
// A single block of empty control bytes for tables without any slots allocated.
// This enables removing a branch in the hot path of find().
// --------------------------------------------------------------------------
inline ctrl_t* EmptyGroup() {
    alignas(16) static constexpr ctrl_t empty_group[] = {kSentinel, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty,
                                                         kEmpty,    kEmpty, kEmpty, kEmpty, kEmpty, kEmpty,
                                                         kEmpty,    kEmpty, kEmpty, kEmpty};
    return const_cast<ctrl_t*>(empty_group);
}

// --------------------------------------------------------------------------
inline size_t HashSeed(const ctrl_t* ctrl) {
    // The low bits of the pointer have little or no entropy because of
    // alignment. We shift the pointer to try to use higher entropy bits. A
    // good number seems to be 12 bits, because that aligns with page size.
    return reinterpret_cast<uintptr_t>(ctrl) >> 12;
}

#ifdef PHMAP_NON_DETERMINISTIC

inline size_t H1(size_t hashval, const ctrl_t* ctrl) {
    // use ctrl_ pointer to add entropy to ensure
    // non-deterministic iteration order.
    return (hashval >> 7) ^ HashSeed(ctrl);
}

#else

inline size_t H1(size_t hashval, const ctrl_t*) {
    return (hashval >> 7);
}

#endif

inline ctrl_t H2(size_t hashval) {
    return (ctrl_t)(hashval & 0x7F);
}

inline bool IsEmpty(ctrl_t c) {
    return c == kEmpty;
}
inline bool IsFull(ctrl_t c) {
    return c >= 0;
}
inline bool IsDeleted(ctrl_t c) {
    return c == kDeleted;
}
inline bool IsEmptyOrDeleted(ctrl_t c) {
    return c < kSentinel;
}

#if PHMAP_HAVE_SSE2

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4365) // conversion from 'int' to 'T', signed/unsigned mismatch
#endif

// --------------------------------------------------------------------------
// https://github.com/abseil/abseil-cpp/issues/209
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=87853
// _mm_cmpgt_epi8 is broken under GCC with -funsigned-char
// Work around this by using the portable implementation of Group
// when using -funsigned-char under GCC.
// --------------------------------------------------------------------------
inline __m128i _mm_cmpgt_epi8_fixed(__m128i a, __m128i b) {
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverflow"

    if (std::is_unsigned<char>::value) {
        const __m128i mask = _mm_set1_epi8(static_cast<char>(0x80));
        const __m128i diff = _mm_subs_epi8(b, a);
        return _mm_cmpeq_epi8(_mm_and_si128(diff, mask), mask);
    }

#pragma GCC diagnostic pop
#endif
    return _mm_cmpgt_epi8(a, b);
}

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
struct GroupSse2Impl {
    enum { kWidth = 16 }; // the number of slots per group

    explicit GroupSse2Impl(const ctrl_t* pos) { ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos)); }

    // Returns a bitmask representing the positions of slots that match hash.
    // ----------------------------------------------------------------------
    BitMask<uint32_t, kWidth> Match(h2_t hash) const {
        auto match = _mm_set1_epi8((char)hash);
        return BitMask<uint32_t, kWidth>(_mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
    }

    // Returns a bitmask representing the positions of empty slots.
    // ------------------------------------------------------------
    BitMask<uint32_t, kWidth> MatchEmpty() const {
#if PHMAP_HAVE_SSSE3
        // This only works because kEmpty is -128.
        return BitMask<uint32_t, kWidth>(_mm_movemask_epi8(_mm_sign_epi8(ctrl, ctrl)));
#else
        return Match(static_cast<h2_t>(kEmpty));
#endif
    }

    // Returns a bitmask representing the positions of empty or deleted slots.
    // -----------------------------------------------------------------------
    BitMask<uint32_t, kWidth> MatchEmptyOrDeleted() const {
        auto special = _mm_set1_epi8(kSentinel);
        return BitMask<uint32_t, kWidth>(_mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)));
    }

    // Returns the number of trailing empty or deleted elements in the group.
    // ----------------------------------------------------------------------
    uint32_t CountLeadingEmptyOrDeleted() const {
        auto special = _mm_set1_epi8(kSentinel);
        return TrailingZeros(_mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)) + 1);
    }

    // ----------------------------------------------------------------------
    void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t* dst) const {
        auto msbs = _mm_set1_epi8(static_cast<char>(-128));
        auto x126 = _mm_set1_epi8(126);
#if PHMAP_HAVE_SSSE3
        auto res = _mm_or_si128(_mm_shuffle_epi8(x126, ctrl), msbs);
#else
        auto zero = _mm_setzero_si128();
        auto special_mask = _mm_cmpgt_epi8_fixed(zero, ctrl);
        auto res = _mm_or_si128(msbs, _mm_andnot_si128(special_mask, x126));
#endif
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), res);
    }

    __m128i ctrl;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // PHMAP_HAVE_SSE2

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
struct GroupPortableImpl {
    enum { kWidth = 8 };

    explicit GroupPortableImpl(const ctrl_t* pos) : ctrl(little_endian::Load64(pos)) {}

    BitMask<uint64_t, kWidth, 3> Match(h2_t hash) const {
        // For the technique, see:
        // http://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
        // (Determine if a word has a byte equal to n).
        //
        // Caveat: there are false positives but:
        // - they only occur if there is a real match
        // - they never occur on kEmpty, kDeleted, kSentinel
        // - they will be handled gracefully by subsequent checks in code
        //
        // Example:
        //   v = 0x1716151413121110
        //   hash = 0x12
        //   retval = (v - lsbs) & ~v & msbs = 0x0000000080800000
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        constexpr uint64_t lsbs = 0x0101010101010101ULL;
        auto x = ctrl ^ (lsbs * hash);
        return BitMask<uint64_t, kWidth, 3>((x - lsbs) & ~x & msbs);
    }

    BitMask<uint64_t, kWidth, 3> MatchEmpty() const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 6)) & msbs);
    }

    BitMask<uint64_t, kWidth, 3> MatchEmptyOrDeleted() const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 7)) & msbs);
    }

    uint32_t CountLeadingEmptyOrDeleted() const {
        constexpr uint64_t gaps = 0x00FEFEFEFEFEFEFEULL;
        return (uint32_t)((TrailingZeros(((~ctrl & (ctrl >> 7)) | gaps) + 1) + 7) >> 3);
    }

    void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t* dst) const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        constexpr uint64_t lsbs = 0x0101010101010101ULL;
        auto x = ctrl & msbs;
        auto res = (~x + (x >> 7)) & ~lsbs;
        little_endian::Store64(dst, res);
    }

    uint64_t ctrl;
};

#if PHMAP_HAVE_SSE2
using Group = GroupSse2Impl;
#else
using Group = GroupPortableImpl;
#endif

template <class Policy, class Hash, class Eq, class Alloc>
class raw_hash_set;

inline bool IsValidCapacity(size_t n) {
    return ((n + 1) & n) == 0 && n > 0;
}

// --------------------------------------------------------------------------
// PRECONDITION:
//   IsValidCapacity(capacity)
//   ctrl[capacity] == kSentinel
//   ctrl[i] != kSentinel for all i < capacity
// Applies mapping for every byte in ctrl:
//   DELETED -> EMPTY
//   EMPTY -> EMPTY
//   FULL -> DELETED
// --------------------------------------------------------------------------
inline void ConvertDeletedToEmptyAndFullToDeleted(ctrl_t* ctrl, size_t capacity) {
    assert(ctrl[capacity] == kSentinel);
    assert(IsValidCapacity(capacity));
    for (ctrl_t* pos = ctrl; pos != ctrl + capacity + 1; pos += Group::kWidth) {
        Group{pos}.ConvertSpecialToEmptyAndFullToDeleted(pos);
    }
    // Copy the cloned ctrl bytes.
    std::memcpy(ctrl + capacity + 1, ctrl, Group::kWidth);
    ctrl[capacity] = kSentinel;
}

// --------------------------------------------------------------------------
// Rounds up the capacity to the next power of 2 minus 1, with a minimum of 1.
// --------------------------------------------------------------------------
inline size_t NormalizeCapacity(size_t n) {
    return n ? ~size_t{} >> LeadingZeros(n) : 1;
}

// --------------------------------------------------------------------------
// We use 7/8th as maximum load factor.
// For 16-wide groups, that gives an average of two empty slots per group.
// --------------------------------------------------------------------------
inline size_t CapacityToGrowth(size_t capacity) {
    assert(IsValidCapacity(capacity));
    // `capacity*7/8`
    PHMAP_IF_CONSTEXPR(Group::kWidth == 8) {
        if (capacity == 7) {
            // x-x/8 does not work when x==7.
            return 6;
        }
    }
    return capacity - capacity / 8;
}

// --------------------------------------------------------------------------
// From desired "growth" to a lowerbound of the necessary capacity.
// Might not be a valid one and required NormalizeCapacity().
// --------------------------------------------------------------------------
inline size_t GrowthToLowerboundCapacity(size_t growth) {
    // `growth*8/7`
    PHMAP_IF_CONSTEXPR(Group::kWidth == 8) {
        if (growth == 7) {
            // x+(x-1)/7 does not work when x==7.
            return 8;
        }
    }
    return growth + static_cast<size_t>((static_cast<int64_t>(growth) - 1) / 7);
}

namespace hashtable_debug_internal {

// If it is a map, call get<0>().
using std::get;
template <typename T, typename = typename T::mapped_type>
auto GetKey(const typename T::value_type& pair, int) -> decltype(get<0>(pair)) {
    return get<0>(pair);
}

// If it is not a map, return the value directly.
template <typename T>
const typename T::key_type& GetKey(const typename T::key_type& key, char) {
    return key;
}

// --------------------------------------------------------------------------
// Containers should specialize this to provide debug information for that
// container.
// --------------------------------------------------------------------------
template <class Container, typename Enabler = void>
struct HashtableDebugAccess {
    // Returns the number of probes required to find `key` in `c`.  The "number of
    // probes" is a concept that can vary by container.  Implementations should
    // return 0 when `key` was found in the minimum number of operations and
    // should increment the result for each non-trivial operation required to find
    // `key`.
    //
    // The default implementation uses the bucket api from the standard and thus
    // works for `std::unordered_*` containers.
    // --------------------------------------------------------------------------
    static size_t GetNumProbes(const Container& c, const typename Container::key_type& key) {
        if (!c.bucket_count()) return {};
        size_t num_probes = 0;
        size_t bucket = c.bucket(key);
        for (auto it = c.begin(bucket), e = c.end(bucket);; ++it, ++num_probes) {
            if (it == e) return num_probes;
            if (c.key_eq()(key, GetKey<Container>(*it, 0))) return num_probes;
        }
    }
};

} // namespace hashtable_debug_internal

// ----------------------------------------------------------------------------
//                    I N F O Z   S T U B S
// ----------------------------------------------------------------------------
struct HashtablezInfo {
    void PrepareForSampling() {}
};

inline void RecordRehashSlow(HashtablezInfo*, size_t) {}

static inline void RecordInsertSlow(HashtablezInfo*, size_t, size_t) {}

static inline void RecordEraseSlow(HashtablezInfo*) {}

static inline HashtablezInfo* SampleSlow(int64_t*) {
    return nullptr;
}
static inline void UnsampleSlow(HashtablezInfo*) {}

// #define PHMAP_USE_CUSTOM_INFO_HANDLE
#ifndef PHMAP_USE_CUSTOM_INFO_HANDLE
class HashtablezInfoHandle {
public:
    inline void RecordStorageChanged(size_t, size_t) {}
    inline void RecordRehash(size_t) {}
    inline void RecordInsert(size_t, size_t) {}
    inline void RecordErase() {}
    friend inline void swap(HashtablezInfoHandle&, HashtablezInfoHandle&) noexcept {}
};

#else

class HashtablezInfoHandle {
public:
    inline void RecordStorageChanged(size_t, size_t) {}
    inline void RecordRehash(size_t) { rehash_number += 1; }
    inline void RecordInsert(size_t hash_val, size_t probe_length) {
        insert_number += 1;
        insert_probe_length += probe_length;
    }
    inline void RecordErase() {}
    friend inline void swap(HashtablezInfoHandle& x, HashtablezInfoHandle& y) noexcept {
        std::swap(x.insert_number, y.insert_number);
        std::swap(x.insert_probe_length, y.insert_probe_length);
        std::swap(x.rehash_number, y.rehash_number);
    }
    size_t insert_number = 0;
    size_t insert_probe_length = 0;
    size_t rehash_number = 0;
};

#endif

static inline HashtablezInfoHandle Sample() {
    return HashtablezInfoHandle();
}

class HashtablezSampler {
public:
    // Returns a global Sampler.
    static HashtablezSampler& Global() {
        static HashtablezSampler hzs;
        return hzs;
    }
    HashtablezInfo* Register() {
        static HashtablezInfo info;
        return &info;
    }
    void Unregister(HashtablezInfo*) {}

    using DisposeCallback = void (*)(const HashtablezInfo&);
    DisposeCallback SetDisposeCallback(DisposeCallback) { return nullptr; }
    int64_t Iterate(const std::function<void(const HashtablezInfo& stack)>&) { return 0; }
};

static inline void SetHashtablezEnabled(bool) {}
static inline void SetHashtablezSampleParameter(int32_t) {}
static inline void SetHashtablezMaxSamples(int32_t) {}

namespace memory_internal {

// Constructs T into uninitialized storage pointed by `ptr` using the args
// specified in the tuple.
// ----------------------------------------------------------------------------
template <class Alloc, class T, class Tuple, size_t... I>
void ConstructFromTupleImpl(Alloc* alloc, T* ptr, Tuple&& t, phmap::index_sequence<I...>) {
    phmap::allocator_traits<Alloc>::construct(*alloc, ptr, std::get<I>(std::forward<Tuple>(t))...);
}

template <class T, class F>
struct WithConstructedImplF {
    template <class... Args>
    decltype(std::declval<F>()(std::declval<T>())) operator()(Args&&... args) const {
        return std::forward<F>(f)(T(std::forward<Args>(args)...));
    }
    F&& f;
};

template <class T, class Tuple, size_t... Is, class F>
decltype(std::declval<F>()(std::declval<T>())) WithConstructedImpl(Tuple&& t, phmap::index_sequence<Is...>, F&& f) {
    return WithConstructedImplF<T, F>{std::forward<F>(f)}(std::get<Is>(std::forward<Tuple>(t))...);
}

template <class T, size_t... Is>
auto TupleRefImpl(T&& t, phmap::index_sequence<Is...>)
        -> decltype(std::forward_as_tuple(std::get<Is>(std::forward<T>(t))...)) {
    return std::forward_as_tuple(std::get<Is>(std::forward<T>(t))...);
}

// Returns a tuple of references to the elements of the input tuple. T must be a
// tuple.
// ----------------------------------------------------------------------------
template <class T>
auto TupleRef(T&& t)
        -> decltype(TupleRefImpl(std::forward<T>(t),
                                 phmap::make_index_sequence<std::tuple_size<typename std::decay<T>::type>::value>())) {
    return TupleRefImpl(std::forward<T>(t),
                        phmap::make_index_sequence<std::tuple_size<typename std::decay<T>::type>::value>());
}

template <class F, class K, class V>
decltype(std::declval<F>()(std::declval<const K&>(), std::piecewise_construct, std::declval<std::tuple<K>>(),
                           std::declval<V>()))
DecomposePairImpl(F&& f, std::pair<std::tuple<K>, V> p) {
    const auto& key = std::get<0>(p.first);
    return std::forward<F>(f)(key, std::piecewise_construct, std::move(p.first), std::move(p.second));
}

} // namespace memory_internal

// ----------------------------------------------------------------------------
//                     R A W _ H A S H _ S E T
// ----------------------------------------------------------------------------
// An open-addressing
// hashtable with quadratic probing.
//
// This is a low level hashtable on top of which different interfaces can be
// implemented, like flat_hash_set, node_hash_set, string_hash_set, etc.
//
// The table interface is similar to that of std::unordered_set. Notable
// differences are that most member functions support heterogeneous keys when
// BOTH the hash and eq functions are marked as transparent. They do so by
// providing a typedef called `is_transparent`.
//
// When heterogeneous lookup is enabled, functions that take key_type act as if
// they have an overload set like:
//
//   iterator find(const key_type& key);
//   template <class K>
//   iterator find(const K& key);
//
//   size_type erase(const key_type& key);
//   template <class K>
//   size_type erase(const K& key);
//
//   std::pair<iterator, iterator> equal_range(const key_type& key);
//   template <class K>
//   std::pair<iterator, iterator> equal_range(const K& key);
//
// When heterogeneous lookup is disabled, only the explicit `key_type` overloads
// exist.
//
// find() also supports passing the hash explicitly:
//
//   iterator find(const key_type& key, size_t hash);
//   template <class U>
//   iterator find(const U& key, size_t hash);
//
// In addition the pointer to element and iterator stability guarantees are
// weaker: all iterators and pointers are invalidated after a new element is
// inserted.
//
// IMPLEMENTATION DETAILS
//
// The table stores elements inline in a slot array. In addition to the slot
// array the table maintains some control state per slot. The extra state is one
// byte per slot and stores empty or deleted marks, or alternatively 7 bits from
// the hash of an occupied slot. The table is split into logical groups of
// slots, like so:
//
//      Group 1         Group 2        Group 3
// +---------------+---------------+---------------+
// | | | | | | | | | | | | | | | | | | | | | | | | |
// +---------------+---------------+---------------+
//
// On lookup the hash is split into two parts:
// - H2: 7 bits (those stored in the control bytes)
// - H1: the rest of the bits
// The groups are probed using H1. For each group the slots are matched to H2 in
// parallel. Because H2 is 7 bits (128 states) and the number of slots per group
// is low (8 or 16) in almost all cases a match in H2 is also a lookup hit.
//
// On insert, once the right group is found (as in lookup), its slots are
// filled in order.
//
// On erase a slot is cleared. In case the group did not have any empty slots
// before the erase, the erased slot is marked as deleted.
//
// Groups without empty slots (but maybe with deleted slots) extend the probe
// sequence. The probing algorithm is quadratic. Given N the number of groups,
// the probing function for the i'th probe is:
//
//   P(0) = H1 % N
//
//   P(i) = (P(i - 1) + i) % N
//
// This probing function guarantees that after N probes, all the groups of the
// table will be probed exactly once.
// ----------------------------------------------------------------------------
template <class Policy, class Hash, class Eq, class Alloc>
class raw_hash_set {
    using PolicyTraits = hash_policy_traits<Policy>;
    using KeyArgImpl = KeyArg<IsTransparent<Eq>::value && IsTransparent<Hash>::value>;

public:
    using init_type = typename PolicyTraits::init_type;
    using key_type = typename PolicyTraits::key_type;
    // TODO(sbenza): Hide slot_type as it is an implementation detail. Needs user
    // code fixes!
    using slot_type = typename PolicyTraits::slot_type;
    using allocator_type = Alloc;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using hasher = Hash;
    using key_equal = Eq;
    using policy_type = Policy;
    using value_type = typename PolicyTraits::value_type;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = typename phmap::allocator_traits<allocator_type>::template rebind_traits<value_type>::pointer;
    using const_pointer =
            typename phmap::allocator_traits<allocator_type>::template rebind_traits<value_type>::const_pointer;

    // Alias used for heterogeneous lookup functions.
    // `key_arg<K>` evaluates to `K` when the functors are transparent and to
    // `key_type` otherwise. It permits template argument deduction on `K` for the
    // transparent case.
    template <class K>
    using key_arg = typename KeyArgImpl::template type<K, key_type>;

private:
    // Give an early error when key_type is not hashable/eq.
    auto KeyTypeCanBeHashed(const Hash& h, const key_type& k) -> decltype(h(k));
    auto KeyTypeCanBeEq(const Eq& eq, const key_type& k) -> decltype(eq(k, k));

    using Layout = phmap::priv::Layout<ctrl_t, slot_type>;

    static Layout MakeLayout(size_t capacity) {
        assert(IsValidCapacity(capacity));
        return Layout(capacity + Group::kWidth + 1, capacity);
    }

    using AllocTraits = phmap::allocator_traits<allocator_type>;
    using SlotAlloc = typename phmap::allocator_traits<allocator_type>::template rebind_alloc<slot_type>;
    using SlotAllocTraits = typename phmap::allocator_traits<allocator_type>::template rebind_traits<slot_type>;

    static_assert(std::is_lvalue_reference<reference>::value, "Policy::element() must return a reference");

    template <typename T>
    struct SameAsElementReference
            : std::is_same<typename std::remove_cv<typename std::remove_reference<reference>::type>::type,
                           typename std::remove_cv<typename std::remove_reference<T>::type>::type> {};

    // An enabler for insert(T&&): T must be convertible to init_type or be the
    // same as [cv] value_type [ref].
    // Note: we separate SameAsElementReference into its own type to avoid using
    // reference unless we need to. MSVC doesn't seem to like it in some
    // cases.
    template <class T>
    using RequiresInsertable = typename std::enable_if<
            phmap::disjunction<std::is_convertible<T, init_type>, SameAsElementReference<T>>::value, int>::type;

    // RequiresNotInit is a workaround for gcc prior to 7.1.
    // See https://godbolt.org/g/Y4xsUh.
    template <class T>
    using RequiresNotInit = typename std::enable_if<!std::is_same<T, init_type>::value, int>::type;

    template <class... Ts>
    using IsDecomposable = IsDecomposable<void, PolicyTraits, Hash, Eq, Ts...>;

public:
    static_assert(std::is_same<pointer, value_type*>::value, "Allocators with custom pointer types are not supported");
    static_assert(std::is_same<const_pointer, const value_type*>::value,
                  "Allocators with custom pointer types are not supported");

    class iterator {
        friend class raw_hash_set;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename raw_hash_set::value_type;
        using reference = phmap::conditional_t<PolicyTraits::constant_iterators::value, const value_type&, value_type&>;
        using pointer = phmap::remove_reference_t<reference>*;
        using difference_type = typename raw_hash_set::difference_type;

        iterator() = default;

        // PRECONDITION: not an end() iterator.
        reference operator*() const { return PolicyTraits::element(slot_); }

        // PRECONDITION: not an end() iterator.
        pointer operator->() const { return &operator*(); }

        // PRECONDITION: not an end() iterator.
        iterator& operator++() {
            ++ctrl_;
            ++slot_;
            skip_empty_or_deleted();
            return *this;
        }
        // PRECONDITION: not an end() iterator.
        iterator operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

#if PHMAP_BIDIRECTIONAL
        // PRECONDITION: not a begin() iterator.
        iterator& operator--() {
            assert(ctrl_);
            do {
                --ctrl_;
                --slot_;
            } while (IsEmptyOrDeleted(*ctrl_));
            return *this;
        }

        // PRECONDITION: not a begin() iterator.
        iterator operator--(int) {
            auto tmp = *this;
            --*this;
            return tmp;
        }
#endif

        friend bool operator==(const iterator& a, const iterator& b) { return a.ctrl_ == b.ctrl_; }
        friend bool operator!=(const iterator& a, const iterator& b) { return !(a == b); }

    private:
        iterator(ctrl_t* ctrl) : ctrl_(ctrl) {} // for end()
        iterator(ctrl_t* ctrl, slot_type* slot) : ctrl_(ctrl), slot_(slot) {}

        void skip_empty_or_deleted() {
            while (IsEmptyOrDeleted(*ctrl_)) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group{ctrl_}.CountLeadingEmptyOrDeleted();
                ctrl_ += shift;
                slot_ += shift;
            }
        }

        ctrl_t* ctrl_ = nullptr;
        // To avoid uninitialized member warnings, put slot_ in an anonymous union.
        // The member is not initialized on singleton and end iterators.
        union {
            slot_type* slot_;
        };
    };

    class const_iterator {
        friend class raw_hash_set;

    public:
        using iterator_category = typename iterator::iterator_category;
        using value_type = typename raw_hash_set::value_type;
        using reference = typename raw_hash_set::const_reference;
        using pointer = typename raw_hash_set::const_pointer;
        using difference_type = typename raw_hash_set::difference_type;

        const_iterator() = default;
        // Implicit construction from iterator.
        const_iterator(iterator i) : inner_(std::move(i)) {}

        reference operator*() const { return *inner_; }
        pointer operator->() const { return inner_.operator->(); }

        const_iterator& operator++() {
            ++inner_;
            return *this;
        }
        const_iterator operator++(int) { return inner_++; }

        friend bool operator==(const const_iterator& a, const const_iterator& b) { return a.inner_ == b.inner_; }
        friend bool operator!=(const const_iterator& a, const const_iterator& b) { return !(a == b); }

    private:
        const_iterator(const ctrl_t* ctrl, const slot_type* slot)
                : inner_(const_cast<ctrl_t*>(ctrl), const_cast<slot_type*>(slot)) {}

        iterator inner_;
    };

    using node_type = node_handle<Policy, hash_policy_traits<Policy>, Alloc>;
    using insert_return_type = InsertReturnType<iterator, node_type>;

    raw_hash_set() noexcept(std::is_nothrow_default_constructible<hasher>::value&&
                                    std::is_nothrow_default_constructible<key_equal>::value&&
                                            std::is_nothrow_default_constructible<allocator_type>::value) {}

    explicit raw_hash_set(size_t bucket_cnt, const hasher& hashfn = hasher(), const key_equal& eq = key_equal(),
                          const allocator_type& alloc = allocator_type())
            : ctrl_(EmptyGroup()), settings_(0, hashfn, eq, alloc) {
        if (bucket_cnt) {
            capacity_ = NormalizeCapacity(bucket_cnt);
            reset_growth_left();
            initialize_slots();
        }
    }

    raw_hash_set(size_t bucket_cnt, const hasher& hashfn, const allocator_type& alloc)
            : raw_hash_set(bucket_cnt, hashfn, key_equal(), alloc) {}

    raw_hash_set(size_t bucket_cnt, const allocator_type& alloc)
            : raw_hash_set(bucket_cnt, hasher(), key_equal(), alloc) {}

    explicit raw_hash_set(const allocator_type& alloc) : raw_hash_set(0, hasher(), key_equal(), alloc) {}

    template <class InputIter>
    raw_hash_set(InputIter first, InputIter last, size_t bucket_cnt = 0, const hasher& hashfn = hasher(),
                 const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : raw_hash_set(bucket_cnt, hashfn, eq, alloc) {
        insert(first, last);
    }

    template <class InputIter>
    raw_hash_set(InputIter first, InputIter last, size_t bucket_cnt, const hasher& hashfn, const allocator_type& alloc)
            : raw_hash_set(first, last, bucket_cnt, hashfn, key_equal(), alloc) {}

    template <class InputIter>
    raw_hash_set(InputIter first, InputIter last, size_t bucket_cnt, const allocator_type& alloc)
            : raw_hash_set(first, last, bucket_cnt, hasher(), key_equal(), alloc) {}

    template <class InputIter>
    raw_hash_set(InputIter first, InputIter last, const allocator_type& alloc)
            : raw_hash_set(first, last, 0, hasher(), key_equal(), alloc) {}

    // Instead of accepting std::initializer_list<value_type> as the first
    // argument like std::unordered_set<value_type> does, we have two overloads
    // that accept std::initializer_list<T> and std::initializer_list<init_type>.
    // This is advantageous for performance.
    //
    //   // Turns {"abc", "def"} into std::initializer_list<std::string>, then
    //   // copies the strings into the set.
    //   std::unordered_set<std::string> s = {"abc", "def"};
    //
    //   // Turns {"abc", "def"} into std::initializer_list<const char*>, then
    //   // copies the strings into the set.
    //   phmap::flat_hash_set<std::string> s = {"abc", "def"};
    //
    // The same trick is used in insert().
    //
    // The enabler is necessary to prevent this constructor from triggering where
    // the copy constructor is meant to be called.
    //
    //   phmap::flat_hash_set<int> a, b{a};
    //
    // RequiresNotInit<T> is a workaround for gcc prior to 7.1.
    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    raw_hash_set(std::initializer_list<T> init, size_t bucket_cnt = 0, const hasher& hashfn = hasher(),
                 const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : raw_hash_set(init.begin(), init.end(), bucket_cnt, hashfn, eq, alloc) {}

    raw_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt = 0, const hasher& hashfn = hasher(),
                 const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : raw_hash_set(init.begin(), init.end(), bucket_cnt, hashfn, eq, alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    raw_hash_set(std::initializer_list<T> init, size_t bucket_cnt, const hasher& hashfn, const allocator_type& alloc)
            : raw_hash_set(init, bucket_cnt, hashfn, key_equal(), alloc) {}

    raw_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt, const hasher& hashfn,
                 const allocator_type& alloc)
            : raw_hash_set(init, bucket_cnt, hashfn, key_equal(), alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    raw_hash_set(std::initializer_list<T> init, size_t bucket_cnt, const allocator_type& alloc)
            : raw_hash_set(init, bucket_cnt, hasher(), key_equal(), alloc) {}

    raw_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt, const allocator_type& alloc)
            : raw_hash_set(init, bucket_cnt, hasher(), key_equal(), alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    raw_hash_set(std::initializer_list<T> init, const allocator_type& alloc)
            : raw_hash_set(init, 0, hasher(), key_equal(), alloc) {}

    raw_hash_set(std::initializer_list<init_type> init, const allocator_type& alloc)
            : raw_hash_set(init, 0, hasher(), key_equal(), alloc) {}

    raw_hash_set(const raw_hash_set& that)
            : raw_hash_set(that, AllocTraits::select_on_container_copy_construction(that.alloc_ref())) {}

    raw_hash_set(const raw_hash_set& that, const allocator_type& a)
            : raw_hash_set(0, that.hash_ref(), that.eq_ref(), a) {
        reserve(that.size());
        // Because the table is guaranteed to be empty, we can do something faster
        // than a full `insert`.
        for (const auto& v : that) {
            const size_t hashval = PolicyTraits::apply(HashElement{hash_ref()}, v);
            auto target = find_first_non_full(hashval);
            set_ctrl(target.offset, H2(hashval));
            emplace_at(target.offset, v);
            infoz_.RecordInsert(hashval, target.probe_length);
        }
        size_ = that.size();
        growth_left() -= that.size();
    }

    raw_hash_set(raw_hash_set&& that) noexcept(
            std::is_nothrow_copy_constructible<hasher>::value&& std::is_nothrow_copy_constructible<key_equal>::value&&
                    std::is_nothrow_copy_constructible<allocator_type>::value)
            : ctrl_(phmap::exchange(that.ctrl_, EmptyGroup())),
              slots_(phmap::exchange(that.slots_, nullptr)),
              size_(phmap::exchange(that.size_, 0)),
              capacity_(phmap::exchange(that.capacity_, 0)),
              infoz_(phmap::exchange(that.infoz_, HashtablezInfoHandle())),
              // Hash, equality and allocator are copied instead of moved because
              // `that` must be left valid. If Hash is std::function<Key>, moving it
              // would create a nullptr functor that cannot be called.
              settings_(that.settings_) {
        // growth_left was copied above, reset the one from `that`.
        that.growth_left() = 0;
    }

    raw_hash_set(raw_hash_set&& that, const allocator_type& a)
            : ctrl_(EmptyGroup()),
              slots_(nullptr),

              settings_(0, that.hash_ref(), that.eq_ref(), a) {
        if (a == that.alloc_ref()) {
            std::swap(ctrl_, that.ctrl_);
            std::swap(slots_, that.slots_);
            std::swap(size_, that.size_);
            std::swap(capacity_, that.capacity_);
            std::swap(growth_left(), that.growth_left());
            std::swap(infoz_, that.infoz_);
        } else {
            reserve(that.size());
            // Note: this will copy elements of dense_set and unordered_set instead of
            // moving them. This can be fixed if it ever becomes an issue.
            for (auto& elem : that) insert(std::move(elem));
        }
    }

    raw_hash_set& operator=(const raw_hash_set& that) {
        raw_hash_set tmp(that,
                         AllocTraits::propagate_on_container_copy_assignment::value ? that.alloc_ref() : alloc_ref());
        swap(tmp);
        return *this;
    }

    raw_hash_set& operator=(raw_hash_set&& that) noexcept(
            phmap::allocator_traits<allocator_type>::is_always_equal::value&& std::is_nothrow_move_assignable<
                    hasher>::value&& std::is_nothrow_move_assignable<key_equal>::value) {
        // TODO(sbenza): We should only use the operations from the noexcept clause
        // to make sure we actually adhere to that contract.
        return move_assign(std::move(that), typename AllocTraits::propagate_on_container_move_assignment());
    }

    ~raw_hash_set() { destroy_slots(); }

    iterator begin() {
        auto it = iterator_at(0);
        it.skip_empty_or_deleted();
        return it;
    }
    iterator end() {
#if PHMAP_BIDIRECTIONAL
        return iterator_at(capacity_);
#else
        return {ctrl_ + capacity_};
#endif
    }

    const_iterator begin() const { return const_cast<raw_hash_set*>(this)->begin(); }
    const_iterator end() const { return const_cast<raw_hash_set*>(this)->end(); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    bool empty() const { return !size(); }
    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
    size_t max_size() const { return (std::numeric_limits<size_t>::max)(); }

    PHMAP_ATTRIBUTE_REINITIALIZES void clear() {
        // Iterating over this container is O(bucket_count()). When bucket_count()
        // is much greater than size(), iteration becomes prohibitively expensive.
        // For clear() it is more important to reuse the allocated array when the
        // container is small because allocation takes comparatively long time
        // compared to destruction of the elements of the container. So we pick the
        // largest bucket_count() threshold for which iteration is still fast and
        // past that we simply deallocate the array.
        if (empty()) return;
        if (capacity_ > 127) {
            destroy_slots();
        } else if (capacity_) {
            for (size_t i = 0; i != capacity_; ++i) {
                if (IsFull(ctrl_[i])) {
                    PolicyTraits::destroy(&alloc_ref(), slots_ + i);
                }
            }
            size_ = 0;
            reset_ctrl();
            reset_growth_left();
        }
        assert(empty());
        infoz_.RecordStorageChanged(0, capacity_);
    }

    // This overload kicks in when the argument is an rvalue of insertable and
    // decomposable type other than init_type.
    //
    //   flat_hash_map<std::string, int> m;
    //   m.insert(std::make_pair("abc", 42));
    template <class T, RequiresInsertable<T> = 0, typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
              T* = nullptr>
    std::pair<iterator, bool> insert(T&& value) {
        return emplace(std::forward<T>(value));
    }

    // This overload kicks in when the argument is a bitfield or an lvalue of
    // insertable and decomposable type.
    //
    //   union { int n : 1; };
    //   flat_hash_set<int> s;
    //   s.insert(n);
    //
    //   flat_hash_set<std::string> s;
    //   const char* p = "hello";
    //   s.insert(p);
    //
    // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
    // RequiresInsertable<T> with RequiresInsertable<const T&>.
    // We are hitting this bug: https://godbolt.org/g/1Vht4f.
    template <class T, RequiresInsertable<T> = 0,
              typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
    std::pair<iterator, bool> insert(const T& value) {
        return emplace(value);
    }

    // This overload kicks in when the argument is an rvalue of init_type. Its
    // purpose is to handle brace-init-list arguments.
    //
    //   flat_hash_set<std::string, int> s;
    //   s.insert({"abc", 42});
    std::pair<iterator, bool> insert(init_type&& value) { return emplace(std::move(value)); }

    template <class T, RequiresInsertable<T> = 0, typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
              T* = nullptr>
    iterator insert(const_iterator, T&& value) {
        return insert(std::forward<T>(value)).first;
    }

    // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
    // RequiresInsertable<T> with RequiresInsertable<const T&>.
    // We are hitting this bug: https://godbolt.org/g/1Vht4f.
    template <class T, RequiresInsertable<T> = 0,
              typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
    iterator insert(const_iterator, const T& value) {
        return insert(value).first;
    }

    iterator insert(const_iterator, init_type&& value) { return insert(std::move(value)).first; }

    template <typename It>
    using IsRandomAccess =
            std::is_same<typename std::iterator_traits<It>::iterator_category, std::random_access_iterator_tag>;

    template <typename T>
    struct has_difference_operator {
    private:
        using yes = std::true_type;
        using no = std::false_type;

        template <typename U>
        static auto test(int) -> decltype(std::declval<U>() - std::declval<U>() == 1, yes());
        template <typename>
        static no test(...);

    public:
        static constexpr bool value = std::is_same<decltype(test<T>(0)), yes>::value;
    };

    template <class InputIt, typename phmap::enable_if_t<has_difference_operator<InputIt>::value, int> = 0>
    void insert(InputIt first, InputIt last) {
        this->reserve(this->size() + (last - first));
        for (; first != last; ++first) emplace(*first);
    }

    template <class InputIt, typename phmap::enable_if_t<!has_difference_operator<InputIt>::value, int> = 0>
    void insert(InputIt first, InputIt last) {
        for (; first != last; ++first) emplace(*first);
    }

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<const T&> = 0>
    void insert(std::initializer_list<T> ilist) {
        insert(ilist.begin(), ilist.end());
    }

    void insert(std::initializer_list<init_type> ilist) { insert(ilist.begin(), ilist.end()); }

    insert_return_type insert(node_type&& node) {
        if (!node) return {end(), false, node_type()};
        const auto& elem = PolicyTraits::element(CommonAccess::GetSlot(node));
        auto res = PolicyTraits::apply(InsertSlot<false>{*this, std::move(*CommonAccess::GetSlot(node))}, elem);
        if (res.second) {
            CommonAccess::Reset(&node);
            return {res.first, true, node_type()};
        } else {
            return {res.first, false, std::move(node)};
        }
    }

    insert_return_type insert(node_type&& node, size_t hashval) {
        if (!node) return {end(), false, node_type()};
        const auto& elem = PolicyTraits::element(CommonAccess::GetSlot(node));
        auto res = PolicyTraits::apply(
                InsertSlotWithHash<false>{*this, std::move(*CommonAccess::GetSlot(node)), hashval}, elem);
        if (res.second) {
            CommonAccess::Reset(&node);
            return {res.first, true, node_type()};
        } else {
            return {res.first, false, std::move(node)};
        }
    }

    iterator insert(const_iterator, node_type&& node) { return insert(std::move(node)).first; }

    // This overload kicks in if we can deduce the key from args. This enables us
    // to avoid constructing value_type if an entry with the same key already
    // exists.
    //
    // For example:
    //
    //   flat_hash_map<std::string, std::string> m = {{"abc", "def"}};
    //   // Creates no std::string copies and makes no heap allocations.
    //   m.emplace("abc", "xyz");
    template <class... Args, typename std::enable_if<IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace(Args&&... args) {
        return PolicyTraits::apply(EmplaceDecomposable{*this}, std::forward<Args>(args)...);
    }

    template <class... Args, typename std::enable_if<IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace_with_hash(size_t hashval, Args&&... args) {
        return PolicyTraits::apply(EmplaceDecomposableHashval{*this, hashval}, std::forward<Args>(args)...);
    }

    // This overload kicks in if we cannot deduce the key from args. It constructs
    // value_type unconditionally and then either moves it into the table or
    // destroys.
    template <class... Args, typename std::enable_if<!IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace(Args&&... args) {
        typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type raw;
        slot_type* slot = reinterpret_cast<slot_type*>(&raw);

        PolicyTraits::construct(&alloc_ref(), slot, std::forward<Args>(args)...);
        const auto& elem = PolicyTraits::element(slot);
        return PolicyTraits::apply(InsertSlot<true>{*this, std::move(*slot)}, elem);
    }

    template <class... Args, typename std::enable_if<!IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace_with_hash(size_t hashval, Args&&... args) {
        typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type raw;
        slot_type* slot = reinterpret_cast<slot_type*>(&raw);

        PolicyTraits::construct(&alloc_ref(), slot, std::forward<Args>(args)...);
        const auto& elem = PolicyTraits::element(slot);
        return PolicyTraits::apply(InsertSlotWithHash<true>{*this, std::move(*slot), hashval}, elem);
    }

    template <class... Args>
    iterator emplace_hint(const_iterator, Args&&... args) {
        return emplace(std::forward<Args>(args)...).first;
    }

    template <class... Args>
    iterator emplace_hint_with_hash(size_t hashval, const_iterator, Args&&... args) {
        return emplace_with_hash(hashval, std::forward<Args>(args)...).first;
    }

    // Extension API: support for lazy emplace.
    //
    // Looks up key in the table. If found, returns the iterator to the element.
    // Otherwise calls f with one argument of type raw_hash_set::constructor. f
    // MUST call raw_hash_set::constructor with arguments as if a
    // raw_hash_set::value_type is constructed, otherwise the behavior is
    // undefined.
    //
    // For example:
    //
    //   std::unordered_set<ArenaString> s;
    //   // Makes ArenaStr even if "abc" is in the map.
    //   s.insert(ArenaString(&arena, "abc"));
    //
    //   flat_hash_set<ArenaStr> s;
    //   // Makes ArenaStr only if "abc" is not in the map.
    //   s.lazy_emplace("abc", [&](const constructor& ctor) {
    //     ctor(&arena, "abc");
    //   });
    //
    // WARNING: This API is currently experimental. If there is a way to implement
    // the same thing with the rest of the API, prefer that.
    class constructor {
        friend class raw_hash_set;

    public:
        template <class... Args>
        void operator()(Args&&... args) const {
            assert(*slot_);
            PolicyTraits::construct(alloc_, *slot_, std::forward<Args>(args)...);
            *slot_ = nullptr;
        }

    private:
        constructor(allocator_type* a, slot_type** slot) : alloc_(a), slot_(slot) {}

        allocator_type* alloc_;
        slot_type** slot_;
    };

    template <class K = key_type, class F>
    iterator lazy_emplace(const key_arg<K>& key, F&& f) {
        auto res = find_or_prepare_insert(key);
        auto iter = iterator_at(res.first);
        if (res.second) {
            try {
                lazy_emplace_at(res.first, std::forward<F>(f));
            } catch (std::bad_alloc const& e) {
                erase(iter);
                throw;
            }
        }
        return iter;
    }

    template <class K = key_type, class F>
    iterator lazy_emplace_with_hash(const key_arg<K>& key, size_t& hashval, F&& f) {
        auto res = find_or_prepare_insert(key, hashval);
        auto iter = iterator_at(res.first);
        if (res.second) {
            try {
                lazy_emplace_at(res.first, std::forward<F>(f));
            } catch (std::bad_alloc const& e) {
                erase(iter);
                throw;
            }
        }
        return iter;
    }

    template <class K = key_type, class F>
    void lazy_emplace_at(size_t& idx, F&& f) {
        slot_type* slot = slots_ + idx;
        std::forward<F>(f)(constructor(&alloc_ref(), &slot));
        assert(!slot);
    }

    // Extension API: support for heterogeneous keys.
    //
    //   std::unordered_set<std::string> s;
    //   // Turns "abc" into std::string.
    //   s.erase("abc");
    //
    //   flat_hash_set<std::string> s;
    //   // Uses "abc" directly without copying it into std::string.
    //   s.erase("abc");
    template <class K = key_type>
    size_type erase(const key_arg<K>& key) {
        auto it = find(key);
        if (it == end()) return 0;
        _erase(it);
        return 1;
    }

    template <class K = key_type>
    size_type erase_with_hash(const key_arg<K>& key, size_t hashval) {
        auto it = find(key, hashval);
        if (it == end()) return 0;
        _erase(it);
        return 1;
    }

    iterator erase(const_iterator cit) { return erase(cit.inner_); }

    // Erases the element pointed to by `it`.  Unlike `std::unordered_set::erase`,
    // this method returns void to reduce algorithmic complexity to O(1).  In
    // order to erase while iterating across a map, use the following idiom (which
    // also works for standard containers):
    //
    // for (auto it = m.begin(), end = m.end(); it != end;) {
    //   if (<pred>) {
    //     m._erase(it++);
    //   } else {
    //     ++it;
    //   }
    // }
    void _erase(iterator it) {
        assert(it != end());
        PolicyTraits::destroy(&alloc_ref(), it.slot_);
        erase_meta_only(it);
    }
    void _erase(const_iterator cit) { _erase(cit.inner_); }

    // This overload is necessary because otherwise erase<K>(const K&) would be
    // a better match if non-const iterator is passed as an argument.
    iterator erase(iterator it) {
        auto res = it;
        ++res;
        _erase(it);
        return res;
    }

    iterator erase(const_iterator first, const_iterator last) {
        while (first != last) {
            _erase(first++);
        }
        return last.inner_;
    }

    // Moves elements from `src` into `this`.
    // If the element already exists in `this`, it is left unmodified in `src`.
    template <typename H, typename E>
    void merge(raw_hash_set<Policy, H, E, Alloc>& src) { // NOLINT
        assert(this != &src);
        for (auto it = src.begin(), e = src.end(); it != e; ++it) {
            if (PolicyTraits::apply(InsertSlot<false>{*this, std::move(*it.slot_)}, PolicyTraits::element(it.slot_))
                        .second) {
                src.erase_meta_only(it);
            }
        }
    }

    template <typename H, typename E>
    void merge(raw_hash_set<Policy, H, E, Alloc>&& src) {
        merge(src);
    }

    node_type extract(const_iterator position) {
        auto node = CommonAccess::Make<node_type>(alloc_ref(), position.inner_.slot_);
        erase_meta_only(position);
        return node;
    }

    template <class K = key_type, typename std::enable_if<!std::is_same<K, iterator>::value, int>::type = 0>
    node_type extract(const key_arg<K>& key) {
        auto it = find(key);
        return it == end() ? node_type() : extract(const_iterator{it});
    }

    void swap(raw_hash_set& that) noexcept(IsNoThrowSwappable<hasher>() && IsNoThrowSwappable<key_equal>() &&
                                           (!AllocTraits::propagate_on_container_swap::value ||
                                            IsNoThrowSwappable<allocator_type>())) {
        using std::swap;
        swap(ctrl_, that.ctrl_);
        swap(slots_, that.slots_);
        swap(size_, that.size_);
        swap(capacity_, that.capacity_);
        swap(growth_left(), that.growth_left());
        swap(hash_ref(), that.hash_ref());
        swap(eq_ref(), that.eq_ref());
        swap(infoz_, that.infoz_);
        if (AllocTraits::propagate_on_container_swap::value) {
            swap(alloc_ref(), that.alloc_ref());
        } else {
            // If the allocators do not compare equal it is officially undefined
            // behavior. We choose to do nothing.
        }
    }

#ifndef PHMAP_NON_DETERMINISTIC
    size_t dump_bound() const;

    template <typename OutputArchive>
    bool dump(OutputArchive&) const;

    template <typename InputArchive>
    bool load(InputArchive&);
#endif

    void rehash(size_t n) {
        if (n == 0 && capacity_ == 0) return;
        if (n == 0 && size_ == 0) {
            destroy_slots();
            infoz_.RecordStorageChanged(0, 0);
            return;
        }
        // bitor is a faster way of doing `max` here. We will round up to the next
        // power-of-2-minus-1, so bitor is good enough.
        auto m = NormalizeCapacity((std::max)(n, size()));
        // n == 0 unconditionally rehashes as per the standard.
        if (n == 0 || m > capacity_) {
            resize(m);
        }
    }

    void reserve(size_t n) { rehash(GrowthToLowerboundCapacity(n)); }

    // Extension API: support for heterogeneous keys.
    //
    //   std::unordered_set<std::string> s;
    //   // Turns "abc" into std::string.
    //   s.count("abc");
    //
    //   ch_set<std::string> s;
    //   // Uses "abc" directly without copying it into std::string.
    //   s.count("abc");
    template <class K = key_type>
    size_t count(const key_arg<K>& key) const {
        return find(key) == end() ? size_t(0) : size_t(1);
    }

    // Issues CPU prefetch instructions for the memory needed to find or insert
    // a key.  Like all lookup functions, this support heterogeneous keys.
    //
    // NOTE: This is a very low level operation and should not be used without
    // specific benchmarks indicating its importance.
    void prefetch_hash(size_t hashval) const {
        (void)hashval;
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
        auto seq = probe(hashval);
        _mm_prefetch((const char*)(ctrl_ + seq.offset()), _MM_HINT_NTA);
        _mm_prefetch((const char*)(slots_ + seq.offset()), _MM_HINT_NTA);
#elif defined(__GNUC__)
        auto seq = probe(hashval);
        __builtin_prefetch(static_cast<const void*>(ctrl_ + seq.offset()));
        __builtin_prefetch(static_cast<const void*>(slots_ + seq.offset()));
#endif // __GNUC__
    }

    template <class K = key_type>
    void prefetch(const key_arg<K>& key) const {
        prefetch_hash(this->hash(key));
    }

    // The API of find() has two extensions.
    //
    // 1. The hash can be passed by the user. It must be equal to the hash of the
    // key.
    //
    // 2. The type of the key argument doesn't have to be key_type. This is so
    // called heterogeneous key support.
    template <class K = key_type>
    iterator find(const key_arg<K>& key, size_t hashval) {
        size_t offset;
        if (find_impl(key, hashval, offset))
            return iterator_at(offset);
        else
            return end();
    }

    template <class K = key_type>
    pointer find_ptr(const key_arg<K>& key, size_t hashval) {
        size_t offset;
        if (find_impl(key, hashval, offset))
            return &PolicyTraits::element(slots_ + offset);
        else
            return nullptr;
    }

    template <class K = key_type>
    iterator find(const key_arg<K>& key) {
        return find(key, this->hash(key));
    }

    template <class K = key_type>
    const_iterator find(const key_arg<K>& key, size_t hashval) const {
        return const_cast<raw_hash_set*>(this)->find(key, hashval);
    }
    template <class K = key_type>
    const_iterator find(const key_arg<K>& key) const {
        return find(key, this->hash(key));
    }

    template <class K = key_type>
    bool contains(const key_arg<K>& key) const {
        return find(key) != end();
    }

    template <class K = key_type>
    bool contains(const key_arg<K>& key, size_t hashval) const {
        return find(key, hashval) != end();
    }

    template <class K = key_type>
    std::pair<iterator, iterator> equal_range(const key_arg<K>& key) {
        auto it = find(key);
        if (it != end()) return {it, std::next(it)};
        return {it, it};
    }
    template <class K = key_type>
    std::pair<const_iterator, const_iterator> equal_range(const key_arg<K>& key) const {
        auto it = find(key);
        if (it != end()) return {it, std::next(it)};
        return {it, it};
    }

    size_t bucket_count() const { return capacity_; }
    float load_factor() const { return capacity_ ? static_cast<double>(size()) / capacity_ : 0.0; }
    float max_load_factor() const { return 1.0f; }
    void max_load_factor(float) {
        // Does nothing.
    }

    hasher hash_function() const {
        return hash_ref();
    } // warning: doesn't match internal hash - use hash() member function
    key_equal key_eq() const { return eq_ref(); }
    allocator_type get_allocator() const { return alloc_ref(); }

    friend bool operator==(const raw_hash_set& a, const raw_hash_set& b) {
        if (a.size() != b.size()) return false;
        const raw_hash_set* outer = &a;
        const raw_hash_set* inner = &b;
        if (outer->capacity() > inner->capacity()) std::swap(outer, inner);
        for (const value_type& elem : *outer)
            if (!inner->has_element(elem)) return false;
        return true;
    }

    friend bool operator!=(const raw_hash_set& a, const raw_hash_set& b) { return !(a == b); }

    friend void swap(raw_hash_set& a, raw_hash_set& b) noexcept(noexcept(a.swap(b))) { a.swap(b); }

    template <class K>
    size_t hash(const K& key) const {
        return HashElement{hash_ref()}(key);
    }

private:
    template <class Container, typename Enabler>
    friend struct phmap::priv::hashtable_debug_internal::HashtableDebugAccess;

    template <class K = key_type>
    bool find_impl(const key_arg<K>& key, size_t hashval, size_t& offset) {
        auto seq = probe(hashval);
        while (true) {
            Group g{ctrl_ + seq.offset()};
            for (int i : g.Match((h2_t)H2(hashval))) {
                offset = seq.offset((size_t)i);
                if (PHMAP_PREDICT_TRUE(PolicyTraits::apply(EqualElement<K>{key, eq_ref()},
                                                           PolicyTraits::element(slots_ + offset))))
                    return true;
            }
            if (PHMAP_PREDICT_TRUE(g.MatchEmpty())) return false;
            seq.next();
        }
    }

    struct FindElement {
        template <class K, class... Args>
        const_iterator operator()(const K& key, Args&&...) const {
            return s.find(key);
        }
        const raw_hash_set& s;
    };

    struct HashElement {
        template <class K, class... Args>
        size_t operator()(const K& key, Args&&...) const {
            return phmap_mix<sizeof(size_t)>()(h(key));
        }
        const hasher& h;
    };

    template <class K1>
    struct EqualElement {
        template <class K2, class... Args>
        bool operator()(const K2& lhs, Args&&...) const {
            return eq(lhs, rhs);
        }
        const K1& rhs;
        const key_equal& eq;
    };

    template <class K, class... Args>
    std::pair<iterator, bool> emplace_decomposable_with_hash(const K& key, size_t hashval, Args&&... args) {
        auto res = find_or_prepare_insert(key, hashval);
        if (res.second) {
            emplace_at(res.first, std::forward<Args>(args)...);
        }
        return {iterator_at(res.first), res.second};
    }

    struct EmplaceDecomposable {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&... args) const {
            return s.emplace_decomposable_with_hash(key, s.hash(key), std::forward<Args>(args)...);
        }
        raw_hash_set& s;
    };

    struct EmplaceDecomposableHashval {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&... args) const {
            return s.emplace_decomposable_with_hash(key, hashval, std::forward<Args>(args)...);
        }
        raw_hash_set& s;
        size_t hashval;
    };

    template <bool do_destroy>
    struct InsertSlot {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&...) && {
            auto res = s.find_or_prepare_insert(key);
            if (res.second) {
                PolicyTraits::transfer(&s.alloc_ref(), s.slots_ + res.first, &slot);
            } else if (do_destroy) {
                PolicyTraits::destroy(&s.alloc_ref(), &slot);
            }
            return {s.iterator_at(res.first), res.second};
        }
        raw_hash_set& s;
        // Constructed slot. Either moved into place or destroyed.
        slot_type&& slot;
    };

    template <bool do_destroy>
    struct InsertSlotWithHash {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&...) && {
            auto res = s.find_or_prepare_insert(key, hashval);
            if (res.second) {
                PolicyTraits::transfer(&s.alloc_ref(), s.slots_ + res.first, &slot);
            } else if (do_destroy) {
                PolicyTraits::destroy(&s.alloc_ref(), &slot);
            }
            return {s.iterator_at(res.first), res.second};
        }
        raw_hash_set& s;
        // Constructed slot. Either moved into place or destroyed.
        slot_type&& slot;
        size_t& hashval;
    };

    // "erases" the object from the container, except that it doesn't actually
    // destroy the object. It only updates all the metadata of the class.
    // This can be used in conjunction with Policy::transfer to move the object to
    // another place.
    void erase_meta_only(const_iterator it) {
        assert(IsFull(*it.inner_.ctrl_) && "erasing a dangling iterator");
        --size_;
        const size_t index = (size_t)(it.inner_.ctrl_ - ctrl_);
        const size_t index_before = (index - Group::kWidth) & capacity_;
        const auto empty_after = Group(it.inner_.ctrl_).MatchEmpty();
        const auto empty_before = Group(ctrl_ + index_before).MatchEmpty();

        // We count how many consecutive non empties we have to the right and to the
        // left of `it`. If the sum is >= kWidth then there is at least one probe
        // window that might have seen a full group.
        bool was_never_full =
                empty_before && empty_after &&
                static_cast<size_t>(empty_after.TrailingZeros() + empty_before.LeadingZeros()) < Group::kWidth;

        set_ctrl(index, was_never_full ? kEmpty : kDeleted);
        growth_left() += was_never_full;
        infoz_.RecordErase();
    }

    void initialize_slots() {
        assert(capacity_);
        if (std::is_same<SlotAlloc, std::allocator<slot_type>>::value && slots_ == nullptr) {
            infoz_ = Sample();
        }

        auto layout = MakeLayout(capacity_);
        char* mem = static_cast<char*>(Allocate<Layout::Alignment()>(&alloc_ref(), layout.AllocSize()));
        ctrl_ = reinterpret_cast<ctrl_t*>(layout.template Pointer<0>(mem));
        slots_ = layout.template Pointer<1>(mem);
        reset_ctrl();
        reset_growth_left();
        infoz_.RecordStorageChanged(size_, capacity_);
    }

    void destroy_slots() {
        if (!capacity_) return;
        for (size_t i = 0; i != capacity_; ++i) {
            if (IsFull(ctrl_[i])) {
                PolicyTraits::destroy(&alloc_ref(), slots_ + i);
            }
        }
        auto layout = MakeLayout(capacity_);
        // Unpoison before returning the memory to the allocator.
        SanitizerUnpoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
        if (ctrl_ != EmptyGroup()) {
            Deallocate<Layout::Alignment()>(&alloc_ref(), ctrl_, layout.AllocSize());
            ctrl_ = EmptyGroup();
        }
        slots_ = nullptr;
        size_ = 0;
        capacity_ = 0;
        growth_left() = 0;
    }

    void resize(size_t new_capacity) {
        assert(IsValidCapacity(new_capacity));
        auto* old_ctrl = ctrl_;
        auto* old_slots = slots_;
        const size_t old_capacity = capacity_;
        capacity_ = new_capacity;
        try {
            initialize_slots();
        } catch (std::bad_alloc const& e) {
            capacity_ = old_capacity;
            throw;
        }

        for (size_t i = 0; i != old_capacity; ++i) {
            if (IsFull(old_ctrl[i])) {
                size_t hashval = PolicyTraits::apply(HashElement{hash_ref()}, PolicyTraits::element(old_slots + i));
                auto target = find_first_non_full(hashval);
                size_t new_i = target.offset;
                set_ctrl(new_i, H2(hashval));
                PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, old_slots + i);
            }
        }
        if (old_capacity) {
            SanitizerUnpoisonMemoryRegion(old_slots, sizeof(slot_type) * old_capacity);
            auto layout = MakeLayout(old_capacity);
            Deallocate<Layout::Alignment()>(&alloc_ref(), old_ctrl, layout.AllocSize());
        }

        infoz_.RecordRehash(new_capacity);
    }

    void drop_deletes_without_resize() PHMAP_ATTRIBUTE_NOINLINE {
        assert(IsValidCapacity(capacity_));
        assert(!is_small());
        // Algorithm:
        // - mark all DELETED slots as EMPTY
        // - mark all FULL slots as DELETED
        // - for each slot marked as DELETED
        //     hash = Hash(element)
        //     target = find_first_non_full(hash)
        //     if target is in the same group
        //       mark slot as FULL
        //     else if target is EMPTY
        //       transfer element to target
        //       mark slot as EMPTY
        //       mark target as FULL
        //     else if target is DELETED
        //       swap current element with target element
        //       mark target as FULL
        //       repeat procedure for current slot with moved from element (target)
        ConvertDeletedToEmptyAndFullToDeleted(ctrl_, capacity_);
        typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type raw;
        slot_type* slot = reinterpret_cast<slot_type*>(&raw);
        for (size_t i = 0; i != capacity_; ++i) {
            if (!IsDeleted(ctrl_[i])) continue;
            size_t hashval = PolicyTraits::apply(HashElement{hash_ref()}, PolicyTraits::element(slots_ + i));
            auto target = find_first_non_full(hashval);
            size_t new_i = target.offset;

            // Verify if the old and new i fall within the same group wrt the hashval.
            // If they do, we don't need to move the object as it falls already in the
            // best probe we can.
            const auto probe_index = [&](size_t pos) {
                return ((pos - probe(hashval).offset()) & capacity_) / Group::kWidth;
            };

            // Element doesn't move.
            if (PHMAP_PREDICT_TRUE(probe_index(new_i) == probe_index(i))) {
                set_ctrl(i, H2(hashval));
                continue;
            }
            if (IsEmpty(ctrl_[new_i])) {
                // Transfer element to the empty spot.
                // set_ctrl poisons/unpoisons the slots so we have to call it at the
                // right time.
                set_ctrl(new_i, H2(hashval));
                PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slots_ + i);
                set_ctrl(i, kEmpty);
            } else {
                assert(IsDeleted(ctrl_[new_i]));
                set_ctrl(new_i, H2(hashval));
                // Until we are done rehashing, DELETED marks previously FULL slots.
                // Swap i and new_i elements.
                PolicyTraits::transfer(&alloc_ref(), slot, slots_ + i);
                PolicyTraits::transfer(&alloc_ref(), slots_ + i, slots_ + new_i);
                PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slot);
                --i; // repeat
            }
        }
        reset_growth_left();
    }

    void rehash_and_grow_if_necessary() {
        if (capacity_ == 0) {
            resize(1);
        } else if (size() <= CapacityToGrowth(capacity()) / 2) {
            // Squash DELETED without growing if there is enough capacity.
            drop_deletes_without_resize();
        } else {
            // Otherwise grow the container.
            resize(capacity_ * 2 + 1);
        }
    }

    bool has_element(const value_type& elem, size_t hashval) const {
        auto seq = probe(hashval);
        while (true) {
            Group g{ctrl_ + seq.offset()};
            for (int i : g.Match((h2_t)H2(hashval))) {
                if (PHMAP_PREDICT_TRUE(PolicyTraits::element(slots_ + seq.offset((size_t)i)) == elem)) return true;
            }
            if (PHMAP_PREDICT_TRUE(g.MatchEmpty())) return false;
            seq.next();
            assert(seq.getindex() < capacity_ && "full table!");
        }
        return false;
    }

    bool has_element(const value_type& elem) const {
        size_t hashval = PolicyTraits::apply(HashElement{hash_ref()}, elem);
        return has_element(elem, hashval);
    }

    // Probes the raw_hash_set with the probe sequence for hash and returns the
    // pointer to the first empty or deleted slot.
    // NOTE: this function must work with tables having both kEmpty and kDelete
    // in one group. Such tables appears during drop_deletes_without_resize.
    //
    // This function is very useful when insertions happen and:
    // - the input is already a set
    // - there are enough slots
    // - the element with the hash is not in the table
    struct FindInfo {
        size_t offset;
        size_t probe_length;
    };
    FindInfo find_first_non_full(size_t hashval) {
        auto seq = probe(hashval);
        while (true) {
            Group g{ctrl_ + seq.offset()};
            auto mask = g.MatchEmptyOrDeleted();
            if (mask) {
                return {seq.offset((size_t)mask.LowestBitSet()), seq.getindex()};
            }
            assert(seq.getindex() < capacity_ && "full table!");
            seq.next();
        }
    }

    // TODO(alkis): Optimize this assuming *this and that don't overlap.
    raw_hash_set& move_assign(raw_hash_set&& that, std::true_type) {
        raw_hash_set tmp(std::move(that));
        swap(tmp);
        return *this;
    }
    raw_hash_set& move_assign(raw_hash_set&& that, std::false_type) {
        raw_hash_set tmp(std::move(that), alloc_ref());
        swap(tmp);
        return *this;
    }

protected:
    template <class K>
    std::pair<size_t, bool> find_or_prepare_insert(const K& key, size_t hashval) {
        auto seq = probe(hashval);
        while (true) {
            Group g{ctrl_ + seq.offset()};
            for (int i : g.Match((h2_t)H2(hashval))) {
                if (PHMAP_PREDICT_TRUE(PolicyTraits::apply(EqualElement<K>{key, eq_ref()},
                                                           PolicyTraits::element(slots_ + seq.offset((size_t)i)))))
                    return {seq.offset((size_t)i), false};
            }
            if (PHMAP_PREDICT_TRUE(g.MatchEmpty())) break;
            seq.next();
        }
        return {prepare_insert(hashval), true};
    }

    template <class K>
    std::pair<size_t, bool> find_or_prepare_insert(const K& key) {
        return find_or_prepare_insert(key, this->hash(key));
    }

    size_t prepare_insert(size_t hashval) PHMAP_ATTRIBUTE_NOINLINE {
        auto target = find_first_non_full(hashval);
        if (PHMAP_PREDICT_FALSE(growth_left() == 0 && !IsDeleted(ctrl_[target.offset]))) {
            rehash_and_grow_if_necessary();
            target = find_first_non_full(hashval);
        }
        ++size_;
        growth_left() -= IsEmpty(ctrl_[target.offset]);
        set_ctrl(target.offset, H2(hashval));
        infoz_.RecordInsert(hashval, target.probe_length);
        return target.offset;
    }

    // Constructs the value in the space pointed by the iterator. This only works
    // after an unsuccessful find_or_prepare_insert() and before any other
    // modifications happen in the raw_hash_set.
    //
    // PRECONDITION: i is an index returned from find_or_prepare_insert(k), where
    // k is the key decomposed from `forward<Args>(args)...`, and the bool
    // returned by find_or_prepare_insert(k) was true.
    // POSTCONDITION: *m.iterator_at(i) == value_type(forward<Args>(args)...).
    template <class... Args>
    void emplace_at(size_t i, Args&&... args) {
        PolicyTraits::construct(&alloc_ref(), slots_ + i, std::forward<Args>(args)...);

        assert(PolicyTraits::apply(FindElement{*this}, *iterator_at(i)) == iterator_at(i) &&
               "constructed value does not match the lookup key");
    }

    iterator iterator_at(size_t i) { return {ctrl_ + i, slots_ + i}; }
    const_iterator iterator_at(size_t i) const { return {ctrl_ + i, slots_ + i}; }

public:
    const HashtablezInfoHandle& infoz() const { return infoz_; }

private:
    friend struct RawHashSetTestOnlyAccess;

    probe_seq<Group::kWidth> probe(size_t hashval) const {
        return probe_seq<Group::kWidth>(H1(hashval, ctrl_), capacity_);
    }

    // Reset all ctrl bytes back to kEmpty, except the sentinel.
    void reset_ctrl() {
        std::memset(ctrl_, kEmpty, capacity_ + Group::kWidth);
        ctrl_[capacity_] = kSentinel;
        SanitizerPoisonMemoryRegion(slots_, sizeof(slot_type) * capacity_);
    }

    void reset_growth_left() { growth_left() = CapacityToGrowth(capacity()) - size_; }

    // Sets the control byte, and if `i < Group::kWidth`, set the cloned byte at
    // the end too.
    void set_ctrl(size_t i, ctrl_t h) {
        assert(i < capacity_);

        if (IsFull(h)) {
            SanitizerUnpoisonObject(slots_ + i);
        } else {
            SanitizerPoisonObject(slots_ + i);
        }

        ctrl_[i] = h;
        ctrl_[((i - Group::kWidth) & capacity_) + 1 + ((Group::kWidth - 1) & capacity_)] = h;
    }

    size_t& growth_left() { return settings_.template get<0>(); }

    template <size_t N, template <class, class, class, class> class RefSet, class M, class P, class H, class E, class A,
              bool balance>
    friend class parallel_hash_set;

    template <size_t N, template <class, class, class, class> class RefSet, class M, class P, class H, class E, class A,
              bool balance>
    friend class parallel_hash_map;

    // The representation of the object has two modes:
    //  - small: For capacities < kWidth-1
    //  - large: For the rest.
    //
    // Differences:
    //  - In small mode we are able to use the whole capacity. The extra control
    //  bytes give us at least one "empty" control byte to stop the iteration.
    //  This is important to make 1 a valid capacity.
    //
    //  - In small mode only the first `capacity()` control bytes after the
    //  sentinel are valid. The rest contain dummy kEmpty values that do not
    //  represent a real slot. This is important to take into account on
    //  find_first_non_full(), where we never try ShouldInsertBackwards() for
    //  small tables.
    bool is_small() const { return capacity_ < Group::kWidth - 1; }

    hasher& hash_ref() { return settings_.template get<1>(); }
    const hasher& hash_ref() const { return settings_.template get<1>(); }
    key_equal& eq_ref() { return settings_.template get<2>(); }
    const key_equal& eq_ref() const { return settings_.template get<2>(); }
    allocator_type& alloc_ref() { return settings_.template get<3>(); }
    const allocator_type& alloc_ref() const { return settings_.template get<3>(); }

    // TODO(alkis): Investigate removing some of these fields:
    // - ctrl/slots can be derived from each other
    // - size can be moved into the slot array
    ctrl_t* ctrl_ = EmptyGroup(); // [(capacity + 1) * ctrl_t]
    slot_type* slots_ = nullptr;  // [capacity * slot_type]
    size_t size_ = 0;             // number of full slots
    size_t capacity_ = 0;         // total number of slots
    HashtablezInfoHandle infoz_;
    phmap::priv::CompressedTuple<size_t /* growth_left */, hasher, key_equal, allocator_type> settings_{
            0, hasher{}, key_equal{}, allocator_type{}};
};

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <class Policy, class Hash, class Eq, class Alloc>
class raw_hash_map : public raw_hash_set<Policy, Hash, Eq, Alloc> {
    // P is Policy. It's passed as a template argument to support maps that have
    // incomplete types as values, as in unordered_map<K, IncompleteType>.
    // MappedReference<> may be a non-reference type.
    template <class P>
    using MappedReference = decltype(P::value(std::addressof(std::declval<typename raw_hash_map::reference>())));

    // MappedConstReference<> may be a non-reference type.
    template <class P>
    using MappedConstReference =
            decltype(P::value(std::addressof(std::declval<typename raw_hash_map::const_reference>())));

    using KeyArgImpl = KeyArg<IsTransparent<Eq>::value && IsTransparent<Hash>::value>;

    using Base = raw_hash_set<Policy, Hash, Eq, Alloc>;

public:
    using key_type = typename Policy::key_type;
    using mapped_type = typename Policy::mapped_type;
    template <class K>
    using key_arg = typename KeyArgImpl::template type<K, key_type>;

    static_assert(!std::is_reference<key_type>::value);
    // TODO(alkis): remove this assertion and verify that reference mapped_type is
    // supported.
    static_assert(!std::is_reference<mapped_type>::value);

    using iterator = typename raw_hash_map::raw_hash_set::iterator;
    using const_iterator = typename raw_hash_map::raw_hash_set::const_iterator;

    raw_hash_map() = default;
    using Base::raw_hash_set; // use raw_hash_set constructor

    // The last two template parameters ensure that both arguments are rvalues
    // (lvalue arguments are handled by the overloads below). This is necessary
    // for supporting bitfield arguments.
    //
    //   union { int n : 1; };
    //   flat_hash_map<int, int> m;
    //   m.insert_or_assign(n, n);
    template <class K = key_type, class V = mapped_type, K* = nullptr, V* = nullptr>
    std::pair<iterator, bool> insert_or_assign(key_arg<K>&& k, V&& v) {
        return insert_or_assign_impl(std::move(k), std::forward<V>(v));
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr>
    std::pair<iterator, bool> insert_or_assign(key_arg<K>&& k, const V& v) {
        return insert_or_assign_impl(std::move(k), v);
    }

    template <class K = key_type, class V = mapped_type, V* = nullptr>
    std::pair<iterator, bool> insert_or_assign(const key_arg<K>& k, V&& v) {
        return insert_or_assign_impl(k, std::forward<V>(v));
    }

    template <class K = key_type, class V = mapped_type>
    std::pair<iterator, bool> insert_or_assign(const key_arg<K>& k, const V& v) {
        return insert_or_assign_impl(k, v);
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr, V* = nullptr>
    iterator insert_or_assign(const_iterator, key_arg<K>&& k, V&& v) {
        return insert_or_assign(std::move(k), std::forward<V>(v)).first;
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr>
    iterator insert_or_assign(const_iterator, key_arg<K>&& k, const V& v) {
        return insert_or_assign(std::move(k), v).first;
    }

    template <class K = key_type, class V = mapped_type, V* = nullptr>
    iterator insert_or_assign(const_iterator, const key_arg<K>& k, V&& v) {
        return insert_or_assign(k, std::forward<V>(v)).first;
    }

    template <class K = key_type, class V = mapped_type>
    iterator insert_or_assign(const_iterator, const key_arg<K>& k, const V& v) {
        return insert_or_assign(k, v).first;
    }

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0, K* = nullptr>
    std::pair<iterator, bool> try_emplace(key_arg<K>&& k, Args&&... args) {
        return try_emplace_impl(std::move(k), std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0>
    std::pair<iterator, bool> try_emplace(const key_arg<K>& k, Args&&... args) {
        return try_emplace_impl(k, std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args, K* = nullptr>
    iterator try_emplace(const_iterator, key_arg<K>&& k, Args&&... args) {
        return try_emplace(std::move(k), std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class... Args>
    iterator try_emplace(const_iterator, const key_arg<K>& k, Args&&... args) {
        return try_emplace(k, std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class P = Policy>
    MappedReference<P> at(const key_arg<K>& key) {
        auto it = this->find(key);
        if (it == this->end()) phmap::base_internal::ThrowStdOutOfRange("phmap at(): lookup non-existent key");
        return Policy::value(&*it);
    }

    template <class K = key_type, class P = Policy>
    MappedConstReference<P> at(const key_arg<K>& key) const {
        auto it = this->find(key);
        if (it == this->end()) phmap::base_internal::ThrowStdOutOfRange("phmap at(): lookup non-existent key");
        return Policy::value(&*it);
    }

    template <class K = key_type, class P = Policy, K* = nullptr>
    MappedReference<P> operator[](key_arg<K>&& key) {
        return Policy::value(&*try_emplace(std::move(key)).first);
    }

    template <class K = key_type, class P = Policy>
    MappedReference<P> operator[](const key_arg<K>& key) {
        return Policy::value(&*try_emplace(key).first);
    }

private:
    template <class K, class V>
    std::pair<iterator, bool> insert_or_assign_impl(K&& k, V&& v) {
        auto res = this->find_or_prepare_insert(k);
        if (res.second)
            this->emplace_at(res.first, std::forward<K>(k), std::forward<V>(v));
        else
            Policy::value(&*this->iterator_at(res.first)) = std::forward<V>(v);
        return {this->iterator_at(res.first), res.second};
    }

    template <class K = key_type, class... Args>
    std::pair<iterator, bool> try_emplace_impl(K&& k, Args&&... args) {
        auto res = this->find_or_prepare_insert(k);
        if (res.second)
            this->emplace_at(res.first, std::piecewise_construct, std::forward_as_tuple(std::forward<K>(k)),
                             std::forward_as_tuple(std::forward<Args>(args)...));
        return {this->iterator_at(res.first), res.second};
    }
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// Returns "random" seed.
inline size_t RandomSeed() {
#if PHMAP_HAVE_THREAD_LOCAL
    static thread_local size_t counter = 0;
    size_t value = ++counter;
#else  // PHMAP_HAVE_THREAD_LOCAL
    static std::atomic<size_t> counter(0);
    size_t value = counter.fetch_add(1, std::memory_order_relaxed);
#endif // PHMAP_HAVE_THREAD_LOCAL
    return value ^ static_cast<size_t>(reinterpret_cast<uintptr_t>(&counter));
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
template <size_t N, template <class, class, class, class> class RefSet, class Mtx_, class Policy, class Hash, class Eq,
          class Alloc, bool balance = true>
class parallel_hash_set {
    using PolicyTraits = hash_policy_traits<Policy>;
    using KeyArgImpl = KeyArg<IsTransparent<Eq>::value && IsTransparent<Hash>::value>;

    static_assert(N <= 12, "N = 12 means 4096 hash tables!");
    constexpr static size_t num_tables = 1 << N;
    constexpr static size_t mask = num_tables - 1;

public:
    using EmbeddedSet = RefSet<Policy, Hash, Eq, Alloc>;
    using EmbeddedIterator = typename EmbeddedSet::iterator;
    using EmbeddedConstIterator = typename EmbeddedSet::const_iterator;
    using constructor = typename EmbeddedSet::constructor;
    using init_type = typename PolicyTraits::init_type;
    using key_type = typename PolicyTraits::key_type;
    using slot_type = typename PolicyTraits::slot_type;
    using allocator_type = Alloc;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using hasher = Hash;
    using key_equal = Eq;
    using policy_type = Policy;
    using value_type = typename PolicyTraits::value_type;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = typename phmap::allocator_traits<allocator_type>::template rebind_traits<value_type>::pointer;
    using const_pointer =
            typename phmap::allocator_traits<allocator_type>::template rebind_traits<value_type>::const_pointer;

    // Alias used for heterogeneous lookup functions.
    // `key_arg<K>` evaluates to `K` when the functors are transparent and to
    // `key_type` otherwise. It permits template argument deduction on `K` for the
    // transparent case.
    // --------------------------------------------------------------------
    template <class K>
    using key_arg = typename KeyArgImpl::template type<K, key_type>;

protected:
    using Lockable = phmap::LockableImpl<Mtx_>;

    // --------------------------------------------------------------------
    struct Inner : public Lockable {
        bool operator==(const Inner& o) const {
            typename Lockable::SharedLocks l(const_cast<Inner&>(*this), const_cast<Inner&>(o));
            return set_ == o.set_;
        }

        EmbeddedSet set_;
    };

private:
    // Give an early error when key_type is not hashable/eq.
    // --------------------------------------------------------------------
    auto KeyTypeCanBeHashed(const Hash& h, const key_type& k) -> decltype(h(k));
    auto KeyTypeCanBeEq(const Eq& eq, const key_type& k) -> decltype(eq(k, k));

    using AllocTraits = phmap::allocator_traits<allocator_type>;

    static_assert(std::is_lvalue_reference<reference>::value, "Policy::element() must return a reference");

    template <typename T>
    struct SameAsElementReference
            : std::is_same<typename std::remove_cv<typename std::remove_reference<reference>::type>::type,
                           typename std::remove_cv<typename std::remove_reference<T>::type>::type> {};

    // An enabler for insert(T&&): T must be convertible to init_type or be the
    // same as [cv] value_type [ref].
    // Note: we separate SameAsElementReference into its own type to avoid using
    // reference unless we need to. MSVC doesn't seem to like it in some
    // cases.
    // --------------------------------------------------------------------
    template <class T>
    using RequiresInsertable = typename std::enable_if<
            phmap::disjunction<std::is_convertible<T, init_type>, SameAsElementReference<T>>::value, int>::type;

    // RequiresNotInit is a workaround for gcc prior to 7.1.
    // See https://godbolt.org/g/Y4xsUh.
    template <class T>
    using RequiresNotInit = typename std::enable_if<!std::is_same<T, init_type>::value, int>::type;

    template <class... Ts>
    using IsDecomposable = IsDecomposable<void, PolicyTraits, Hash, Eq, Ts...>;

public:
    static_assert(std::is_same<pointer, value_type*>::value, "Allocators with custom pointer types are not supported");
    static_assert(std::is_same<const_pointer, const value_type*>::value,
                  "Allocators with custom pointer types are not supported");

    // --------------------- i t e r a t o r ------------------------------
    class iterator {
        friend class parallel_hash_set;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename parallel_hash_set::value_type;
        using reference = phmap::conditional_t<PolicyTraits::constant_iterators::value, const value_type&, value_type&>;
        using pointer = phmap::remove_reference_t<reference>*;
        using difference_type = typename parallel_hash_set::difference_type;
        using Inner = typename parallel_hash_set::Inner;
        using EmbeddedSet = typename parallel_hash_set::EmbeddedSet;
        using EmbeddedIterator = typename EmbeddedSet::iterator;

        iterator() = default;

        reference operator*() const { return *it_; }
        pointer operator->() const { return &operator*(); }

        iterator& operator++() {
            assert(inner_); // null inner means we are already at the end
            ++it_;
            skip_empty();
            return *this;
        }

        iterator operator++(int) {
            assert(inner_); // null inner means we are already at the end
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        friend bool operator==(const iterator& a, const iterator& b) {
            return a.inner_ == b.inner_ && (!a.inner_ || a.it_ == b.it_);
        }

        friend bool operator!=(const iterator& a, const iterator& b) { return !(a == b); }

    private:
        iterator(Inner* inner, Inner* inner_end, const EmbeddedIterator& it)
                : inner_(inner), inner_end_(inner_end), it_(it) { // for begin() and end()
            if (inner) it_end_ = inner->set_.end();
        }

        void skip_empty() {
            while (it_ == it_end_) {
                ++inner_;
                if (inner_ == inner_end_) {
                    inner_ = nullptr; // marks end()
                    break;
                } else {
                    it_ = inner_->set_.begin();
                    it_end_ = inner_->set_.end();
                }
            }
        }

        Inner* inner_ = nullptr;
        Inner* inner_end_ = nullptr;
        EmbeddedIterator it_, it_end_;
    };

    // --------------------- c o n s t   i t e r a t o r -----------------
    class const_iterator {
        friend class parallel_hash_set;

    public:
        using iterator_category = typename iterator::iterator_category;
        using value_type = typename parallel_hash_set::value_type;
        using reference = typename parallel_hash_set::const_reference;
        using pointer = typename parallel_hash_set::const_pointer;
        using difference_type = typename parallel_hash_set::difference_type;
        using Inner = typename parallel_hash_set::Inner;

        const_iterator() = default;
        // Implicit construction from iterator.
        const_iterator(iterator i) : iter_(std::move(i)) {}

        reference operator*() const { return *(iter_); }
        pointer operator->() const { return iter_.operator->(); }

        const_iterator& operator++() {
            ++iter_;
            return *this;
        }
        const_iterator operator++(int) { return iter_++; }

        friend bool operator==(const const_iterator& a, const const_iterator& b) { return a.iter_ == b.iter_; }
        friend bool operator!=(const const_iterator& a, const const_iterator& b) { return !(a == b); }

    private:
        const_iterator(const Inner* inner, const Inner* inner_end, const EmbeddedIterator& it)
                : iter_(const_cast<Inner**>(inner), const_cast<Inner**>(inner_end), const_cast<EmbeddedIterator*>(it)) {
        }

        iterator iter_;
    };

    using node_type = node_handle<Policy, hash_policy_traits<Policy>, Alloc>;
    using insert_return_type = InsertReturnType<iterator, node_type>;

    // ------------------------- c o n s t r u c t o r s ------------------

    parallel_hash_set() noexcept(std::is_nothrow_default_constructible<hasher>::value&&
                                         std::is_nothrow_default_constructible<key_equal>::value&&
                                                 std::is_nothrow_default_constructible<allocator_type>::value) {}

    explicit parallel_hash_set(size_t bucket_cnt, const hasher& hash_param = hasher(),
                               const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type()) {
        for (auto& inner : sets_) inner.set_ = EmbeddedSet(bucket_cnt / N, hash_param, eq, alloc);
    }

    parallel_hash_set(size_t bucket_cnt, const hasher& hash_param, const allocator_type& alloc)
            : parallel_hash_set(bucket_cnt, hash_param, key_equal(), alloc) {}

    parallel_hash_set(size_t bucket_cnt, const allocator_type& alloc)
            : parallel_hash_set(bucket_cnt, hasher(), key_equal(), alloc) {}

    explicit parallel_hash_set(const allocator_type& alloc) : parallel_hash_set(0, hasher(), key_equal(), alloc) {}

    template <class InputIter>
    parallel_hash_set(InputIter first, InputIter last, size_t bucket_cnt = 0, const hasher& hash_param = hasher(),
                      const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : parallel_hash_set(bucket_cnt, hash_param, eq, alloc) {
        insert(first, last);
    }

    template <class InputIter>
    parallel_hash_set(InputIter first, InputIter last, size_t bucket_cnt, const hasher& hash_param,
                      const allocator_type& alloc)
            : parallel_hash_set(first, last, bucket_cnt, hash_param, key_equal(), alloc) {}

    template <class InputIter>
    parallel_hash_set(InputIter first, InputIter last, size_t bucket_cnt, const allocator_type& alloc)
            : parallel_hash_set(first, last, bucket_cnt, hasher(), key_equal(), alloc) {}

    template <class InputIter>
    parallel_hash_set(InputIter first, InputIter last, const allocator_type& alloc)
            : parallel_hash_set(first, last, 0, hasher(), key_equal(), alloc) {}

    // Instead of accepting std::initializer_list<value_type> as the first
    // argument like std::unordered_set<value_type> does, we have two overloads
    // that accept std::initializer_list<T> and std::initializer_list<init_type>.
    // This is advantageous for performance.
    //
    //   // Turns {"abc", "def"} into std::initializer_list<std::string>, then copies
    //   // the strings into the set.
    //   std::unordered_set<std::string> s = {"abc", "def"};
    //
    //   // Turns {"abc", "def"} into std::initializer_list<const char*>, then
    //   // copies the strings into the set.
    //   phmap::flat_hash_set<std::string> s = {"abc", "def"};
    //
    // The same trick is used in insert().
    //
    // The enabler is necessary to prevent this constructor from triggering where
    // the copy constructor is meant to be called.
    //
    //   phmap::flat_hash_set<int> a, b{a};
    //
    // RequiresNotInit<T> is a workaround for gcc prior to 7.1.
    // --------------------------------------------------------------------
    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    parallel_hash_set(std::initializer_list<T> init, size_t bucket_cnt = 0, const hasher& hash_param = hasher(),
                      const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : parallel_hash_set(init.begin(), init.end(), bucket_cnt, hash_param, eq, alloc) {}

    parallel_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt = 0, const hasher& hash_param = hasher(),
                      const key_equal& eq = key_equal(), const allocator_type& alloc = allocator_type())
            : parallel_hash_set(init.begin(), init.end(), bucket_cnt, hash_param, eq, alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    parallel_hash_set(std::initializer_list<T> init, size_t bucket_cnt, const hasher& hash_param,
                      const allocator_type& alloc)
            : parallel_hash_set(init, bucket_cnt, hash_param, key_equal(), alloc) {}

    parallel_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt, const hasher& hash_param,
                      const allocator_type& alloc)
            : parallel_hash_set(init, bucket_cnt, hash_param, key_equal(), alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    parallel_hash_set(std::initializer_list<T> init, size_t bucket_cnt, const allocator_type& alloc)
            : parallel_hash_set(init, bucket_cnt, hasher(), key_equal(), alloc) {}

    parallel_hash_set(std::initializer_list<init_type> init, size_t bucket_cnt, const allocator_type& alloc)
            : parallel_hash_set(init, bucket_cnt, hasher(), key_equal(), alloc) {}

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<T> = 0>
    parallel_hash_set(std::initializer_list<T> init, const allocator_type& alloc)
            : parallel_hash_set(init, 0, hasher(), key_equal(), alloc) {}

    parallel_hash_set(std::initializer_list<init_type> init, const allocator_type& alloc)
            : parallel_hash_set(init, 0, hasher(), key_equal(), alloc) {}

    parallel_hash_set(const parallel_hash_set& that)
            : parallel_hash_set(that, AllocTraits::select_on_container_copy_construction(that.alloc_ref())) {}

    parallel_hash_set(const parallel_hash_set& that, const allocator_type& a)
            : parallel_hash_set(0, that.hash_ref(), that.eq_ref(), a) {
        for (size_t i = 0; i < num_tables; ++i) sets_[i].set_ = {that.sets_[i].set_, a};
    }

    parallel_hash_set(parallel_hash_set&& that) noexcept(
            std::is_nothrow_copy_constructible<hasher>::value&& std::is_nothrow_copy_constructible<key_equal>::value&&
                    std::is_nothrow_copy_constructible<allocator_type>::value)
            : parallel_hash_set(std::move(that), that.alloc_ref()) {}

    parallel_hash_set(parallel_hash_set&& that, const allocator_type& a) {
        for (size_t i = 0; i < num_tables; ++i) sets_[i].set_ = {std::move(that.sets_[i]).set_, a};
    }

    parallel_hash_set& operator=(const parallel_hash_set& that) {
        for (size_t i = 0; i < num_tables; ++i) sets_[i].set_ = that.sets_[i].set_;
        return *this;
    }

    parallel_hash_set& operator=(parallel_hash_set&& that) noexcept(
            phmap::allocator_traits<allocator_type>::is_always_equal::value&& std::is_nothrow_move_assignable<
                    hasher>::value&& std::is_nothrow_move_assignable<key_equal>::value) {
        for (size_t i = 0; i < num_tables; ++i) sets_[i].set_ = std::move(that.sets_[i].set_);
        return *this;
    }

    ~parallel_hash_set() = default;

    iterator begin() {
        auto it = iterator(&sets_[0], &sets_[0] + num_tables, sets_[0].set_.begin());
        it.skip_empty();
        return it;
    }

    iterator end() { return iterator(); }
    const_iterator begin() const { return const_cast<parallel_hash_set*>(this)->begin(); }
    const_iterator end() const { return const_cast<parallel_hash_set*>(this)->end(); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    bool empty() const { return !size(); }

    size_t size() const {
        size_t sz = 0;
        for (const auto& inner : sets_) sz += inner.set_.size();
        return sz;
    }

    size_t capacity() const {
        size_t c = 0;
        for (const auto& inner : sets_) c += inner.set_.capacity();
        return c;
    }

    std::vector<size_t> capacities() const {
        std::vector<size_t> ret;
        ret.resize(sets_.size());
        for (size_t i = 0; i < sets_.size(); i++) {
            ret[i] = sets_[i].set_.capacity();
        }
        return ret;
    }

    size_t max_size() const { return (std::numeric_limits<size_t>::max)(); }

    PHMAP_ATTRIBUTE_REINITIALIZES void clear() {
        for (auto& inner : sets_) {
            typename Lockable::UniqueLock m(inner);
            inner.set_.clear();
        }
    }

    // extension - clears only soecified submap
    // ----------------------------------------
    void clear(std::size_t submap_index) {
        Inner& inner = sets_[submap_index];
        typename Lockable::UniqueLock m(inner);
        inner.set_.clear();
    }

    // This overload kicks in when the argument is an rvalue of insertable and
    // decomposable type other than init_type.
    //
    //   flat_hash_map<std::string, int> m;
    //   m.insert(std::make_pair("abc", 42));
    // --------------------------------------------------------------------
    template <class T, RequiresInsertable<T> = 0, typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
              T* = nullptr>
    std::pair<iterator, bool> insert(T&& value) {
        return emplace(std::forward<T>(value));
    }

    // This overload kicks in when the argument is a bitfield or an lvalue of
    // insertable and decomposable type.
    //
    //   union { int n : 1; };
    //   flat_hash_set<int> s;
    //   s.insert(n);
    //
    //   flat_hash_set<std::string> s;
    //   const char* p = "hello";
    //   s.insert(p);
    //
    // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
    // RequiresInsertable<T> with RequiresInsertable<const T&>.
    // We are hitting this bug: https://godbolt.org/g/1Vht4f.
    // --------------------------------------------------------------------
    template <class T, RequiresInsertable<T> = 0,
              typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
    std::pair<iterator, bool> insert(const T& value) {
        return emplace(value);
    }

    // This overload kicks in when the argument is an rvalue of init_type. Its
    // purpose is to handle brace-init-list arguments.
    //
    //   flat_hash_set<std::pair<std::string, int>> s;
    //   s.insert({"abc", 42});
    // --------------------------------------------------------------------
    std::pair<iterator, bool> insert(init_type&& value) { return emplace(std::move(value)); }

    template <class T, RequiresInsertable<T> = 0, typename std::enable_if<IsDecomposable<T>::value, int>::type = 0,
              T* = nullptr>
    iterator insert(const_iterator, T&& value) {
        return insert(std::forward<T>(value)).first;
    }

    // TODO(romanp): Once we stop supporting gcc 5.1 and below, replace
    // RequiresInsertable<T> with RequiresInsertable<const T&>.
    // We are hitting this bug: https://godbolt.org/g/1Vht4f.
    // --------------------------------------------------------------------
    template <class T, RequiresInsertable<T> = 0,
              typename std::enable_if<IsDecomposable<const T&>::value, int>::type = 0>
    iterator insert(const_iterator, const T& value) {
        return insert(value).first;
    }

    iterator insert(const_iterator, init_type&& value) { return insert(std::move(value)).first; }

    template <class InputIt>
    void insert(InputIt first, InputIt last) {
        for (; first != last; ++first) insert(*first);
    }

    template <class T, RequiresNotInit<T> = 0, RequiresInsertable<const T&> = 0>
    void insert(std::initializer_list<T> ilist) {
        insert(ilist.begin(), ilist.end());
    }

    void insert(std::initializer_list<init_type> ilist) { insert(ilist.begin(), ilist.end()); }

    insert_return_type insert(node_type&& node) {
        if (!node) return {end(), false, node_type()};
        auto& key = node.key();
        size_t hashval = this->hash(key);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;

        typename Lockable::UniqueLock m(inner);
        auto res = set.insert(std::move(node), hashval);
        return {make_iterator(&inner, res.position), res.inserted, res.inserted ? node_type() : std::move(res.node)};
    }

    iterator insert(const_iterator, node_type&& node) { return insert(std::move(node)).first; }

    struct ReturnKey_ {
        template <class Key, class... Args>
        Key operator()(Key&& k, const Args&...) const {
            return std::forward<Key>(k);
        }
    };

    // --------------------------------------------------------------------
    // phmap expension: emplace_with_hash
    // ----------------------------------
    // same as emplace, but hashval is provided
    // --------------------------------------------------------------------
    template <class K, class... Args>
    std::pair<iterator, bool> emplace_decomposable_with_hash(const K& key, size_t hashval, Args&&... args) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        return make_rv(&inner, set.emplace_decomposable_with_hash(key, hashval, std::forward<Args>(args)...));
    }

    struct EmplaceDecomposableHashval {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&... args) const {
            return s.emplace_decomposable_with_hash(key, hashval, std::forward<Args>(args)...);
        }
        parallel_hash_set& s;
        size_t hashval;
    };

    // This overload kicks in if we can deduce the key from args. This enables us
    // to avoid constructing value_type if an entry with the same key already
    // exists.
    //
    // For example:
    //
    //   flat_hash_map<std::string, std::string> m = {{"abc", "def"}};
    //   // Creates no std::string copies and makes no heap allocations.
    //   m.emplace("abc", "xyz");
    // --------------------------------------------------------------------
    template <class... Args, typename std::enable_if<IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace_with_hash(size_t hashval, Args&&... args) {
        return PolicyTraits::apply(EmplaceDecomposableHashval{*this, hashval}, std::forward<Args>(args)...);
    }

    // This overload kicks in if we cannot deduce the key from args. It constructs
    // value_type unconditionally and then either moves it into the table or
    // destroys.
    // --------------------------------------------------------------------
    template <class... Args, typename std::enable_if<!IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace_with_hash(size_t hashval, Args&&... args) {
        typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type raw;
        slot_type* slot = reinterpret_cast<slot_type*>(&raw);

        PolicyTraits::construct(&alloc_ref(), slot, std::forward<Args>(args)...);
        const auto& elem = PolicyTraits::element(slot);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        typename EmbeddedSet::template InsertSlotWithHash<true> f{inner, std::move(*slot), hashval};
        return make_rv(PolicyTraits::apply(f, elem));
    }

    template <class... Args>
    iterator emplace_hint_with_hash(size_t hashval, const_iterator, Args&&... args) {
        return emplace_with_hash(hashval, std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class F>
    iterator lazy_emplace_with_hash(const key_arg<K>& key, size_t hashval, F&& f) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        return make_iterator(&inner, set.lazy_emplace_with_hash(key, hashval, std::forward<F>(f)));
    }

    // --------------------------------------------------------------------
    // end of phmap expension
    // --------------------------------------------------------------------

    template <class K, class... Args>
    std::pair<iterator, bool> emplace_decomposable(const K& key, Args&&... args) {
        size_t hashval = this->hash(key);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        return make_rv(&inner, set.emplace_decomposable_with_hash(key, hashval, std::forward<Args>(args)...));
    }

    struct EmplaceDecomposable {
        template <class K, class... Args>
        std::pair<iterator, bool> operator()(const K& key, Args&&... args) const {
            return s.emplace_decomposable(key, std::forward<Args>(args)...);
        }
        parallel_hash_set& s;
    };

    // This overload kicks in if we can deduce the key from args. This enables us
    // to avoid constructing value_type if an entry with the same key already
    // exists.
    //
    // For example:
    //
    //   flat_hash_map<std::string, std::string> m = {{"abc", "def"}};
    //   // Creates no std::string copies and makes no heap allocations.
    //   m.emplace("abc", "xyz");
    // --------------------------------------------------------------------
    template <class... Args, typename std::enable_if<IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace(Args&&... args) {
        return PolicyTraits::apply(EmplaceDecomposable{*this}, std::forward<Args>(args)...);
    }

    // This overload kicks in if we cannot deduce the key from args. It constructs
    // value_type unconditionally and then either moves it into the table or
    // destroys.
    // --------------------------------------------------------------------
    template <class... Args, typename std::enable_if<!IsDecomposable<Args...>::value, int>::type = 0>
    std::pair<iterator, bool> emplace(Args&&... args) {
        typename std::aligned_storage<sizeof(slot_type), alignof(slot_type)>::type raw;
        slot_type* slot = reinterpret_cast<slot_type*>(&raw);
        size_t hashval = this->hash(PolicyTraits::key(slot));

        PolicyTraits::construct(&alloc_ref(), slot, std::forward<Args>(args)...);
        const auto& elem = PolicyTraits::element(slot);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        typename EmbeddedSet::template InsertSlotWithHash<true> f{inner, std::move(*slot), hashval};
        return make_rv(PolicyTraits::apply(f, elem));
    }

    template <class... Args>
    iterator emplace_hint(const_iterator, Args&&... args) {
        return emplace(std::forward<Args>(args)...).first;
    }

    iterator make_iterator(Inner* inner, const EmbeddedIterator it) {
        if (it == inner->set_.end()) return iterator();
        return iterator(inner, &sets_[0] + num_tables, it);
    }

    std::pair<iterator, bool> make_rv(Inner* inner, const std::pair<EmbeddedIterator, bool>& res) {
        return {iterator(inner, &sets_[0] + num_tables, res.first), res.second};
    }

    template <class K = key_type, class F>
    iterator lazy_emplace(const key_arg<K>& key, F&& f) {
        auto hashval = this->hash(key);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UniqueLock m(inner);
        return make_iterator(&inner, set.lazy_emplace_with_hash(key, hashval, std::forward<F>(f)));
    }

    template <class K = key_type, class FExists, class FEmplace>
    bool lazy_emplace_l(const key_arg<K>& key, FExists&& fExists, FEmplace&& fEmplace) {
        typename Lockable::UniqueLock m;
        auto res = this->find_or_prepare_insert(key, m);
        Inner* inner = std::get<0>(res);
        if (std::get<2>(res))
            inner->set_.lazy_emplace_at(std::get<1>(res), std::forward<FEmplace>(fEmplace));
        else {
            auto it = this->iterator_at(inner, inner->set_.iterator_at(std::get<1>(res)));
            std::forward<FExists>(fExists)(Policy::value(&*it));
        }
        return std::get<2>(res);
    }

    // Extension API: support for heterogeneous keys.
    //
    //   std::unordered_set<std::string> s;
    //   // Turns "abc" into std::string.
    //   s.erase("abc");
    //
    //   flat_hash_set<std::string> s;
    //   // Uses "abc" directly without copying it into std::string.
    //   s.erase("abc");
    // --------------------------------------------------------------------
    template <class K = key_type>
    size_type erase(const key_arg<K>& key) {
        auto hashval = this->hash(key);
        return erase_with_hash(key, hashval);
    }

    template <class K = key_type>
    size_type erase_with_hash(const key_arg<K>& key, size_t hashval) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::UpgradeLock m(inner);
        auto it = set.find(key, hashval);
        if (it == set.end()) return 0;

        typename Lockable::UpgradeToUnique unique(m);
        set._erase(it);
        return 1;
    }

    // --------------------------------------------------------------------
    iterator erase(const_iterator cit) { return erase(cit.iter_); }

    // Erases the element pointed to by `it`.  Unlike `std::unordered_set::erase`,
    // this method returns void to reduce algorithmic complexity to O(1).  In
    // order to erase while iterating across a map, use the following idiom (which
    // also works for standard containers):
    //
    // for (auto it = m.begin(), end = m.end(); it != end;) {
    //   if (<pred>) {
    //     m._erase(it++);
    //   } else {
    //     ++it;
    //   }
    // }
    // --------------------------------------------------------------------
    void _erase(iterator it) {
        assert(it.inner_ != nullptr);
        it.inner_->set_._erase(it.it_);
    }
    void _erase(const_iterator cit) { _erase(cit.iter_); }

    // This overload is necessary because otherwise erase<K>(const K&) would be
    // a better match if non-const iterator is passed as an argument.
    // --------------------------------------------------------------------
    iterator erase(iterator it) {
        _erase(it++);
        return it;
    }

    iterator erase(const_iterator first, const_iterator last) {
        while (first != last) {
            _erase(first++);
        }
        return last.iter_;
    }

    // Moves elements from `src` into `this`.
    // If the element already exists in `this`, it is left unmodified in `src`.
    // --------------------------------------------------------------------
    template <typename E = Eq>
    void merge(parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, E, Alloc>& src) { // NOLINT
        assert(this != &src);
        if (this != &src) {
            for (size_t i = 0; i < num_tables; ++i) {
                typename Lockable::UniqueLocks l(sets_[i], src.sets_[i]);
                sets_[i].set_.merge(src.sets_[i].set_);
            }
        }
    }

    template <typename E = Eq>
    void merge(parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, E, Alloc>&& src) {
        merge(src);
    }

    node_type extract(const_iterator position) {
        return position.iter_.inner_->set_.extract(EmbeddedConstIterator(position.iter_.it_));
    }

    template <class K = key_type, typename std::enable_if<!std::is_same<K, iterator>::value, int>::type = 0>
    node_type extract(const key_arg<K>& key) {
        auto it = find(key);
        return it == end() ? node_type() : extract(const_iterator{it});
    }

    void swap(parallel_hash_set& that) noexcept(IsNoThrowSwappable<EmbeddedSet>() &&
                                                (!AllocTraits::propagate_on_container_swap::value ||
                                                 IsNoThrowSwappable<allocator_type>())) {
        using std::swap;
        for (size_t i = 0; i < num_tables; ++i) {
            typename Lockable::UniqueLocks l(sets_[i], that.sets_[i]);
            swap(sets_[i].set_, that.sets_[i].set_);
        }
    }

    void rehash(size_t n) {
        if constexpr (balance) {
            size_t nn = n / num_tables;
            for (auto& inner : sets_) {
                typename Lockable::UniqueLock m(inner);
                inner.set_.rehash(nn);
            }
        } else {
            constexpr static uint8_t subratio[num_tables] = {8,  9,  10, 11, 12, 13, 14, 15,
                                                             17, 18, 19, 20, 21, 22, 23, 24};
            for (uint8_t i = 0; i < 16; i++) {
                size_t nn = n * subratio[i] / 256;
                auto& inner = sets_[i];
                typename Lockable::UniqueLock m(inner);
                inner.set_.rehash(nn);
            }
        }
    }

    void reserve(size_t n) {
        if constexpr (balance) {
            size_t target = GrowthToLowerboundCapacity(n);
            size_t normalized = 16 * NormalizeCapacity(n / num_tables);
            rehash(normalized > target ? normalized : target);
        } else {
            rehash(n);
        }
    }

    // Extension API: support for heterogeneous keys.
    //
    //   std::unordered_set<std::string> s;
    //   // Turns "abc" into std::string.
    //   s.count("abc");
    //
    //   ch_set<std::string> s;
    //   // Uses "abc" directly without copying it into std::string.
    //   s.count("abc");
    // --------------------------------------------------------------------
    template <class K = key_type>
    size_t count(const key_arg<K>& key) const {
        return find(key) == end() ? 0 : 1;
    }

    // Issues CPU prefetch instructions for the memory needed to find or insert
    // a key.  Like all lookup functions, this support heterogeneous keys.
    //
    // NOTE: This is a very low level operation and should not be used without
    // specific benchmarks indicating its importance.
    // --------------------------------------------------------------------
    template <class K = key_type>
    void prefetch(const key_arg<K>& key) const {
        (void)key;
        size_t hashval = this->hash(key);
        const Inner& inner = sets_[subidx(hashval)];
        const auto& set = inner.set_;
        typename Lockable::SharedLock m(const_cast<Inner&>(inner));
        set.prefetch_hash(hashval);
    }

    void prefetch_hash(size_t hashval) const {
        const Inner& inner = sets_[subidx(hashval)];
        const auto& set = inner.set_;
        typename Lockable::SharedLock m(const_cast<Inner&>(inner));
        set.prefetch_hash(hashval);
    }

    // The API of find() has two extensions.
    //
    // 1. The hash can be passed by the user. It must be equal to the hash of the
    // key.
    //
    // 2. The type of the key argument doesn't have to be key_type. This is so
    // called heterogeneous key support.
    // --------------------------------------------------------------------
    template <class K = key_type>
    iterator find(const key_arg<K>& key, size_t hashval) {
        typename Lockable::SharedLock m;
        return find(key, hashval, m);
    }

    template <class K = key_type>
    iterator find(const key_arg<K>& key) {
        return find(key, this->hash(key));
    }

    template <class K = key_type>
    const_iterator find(const key_arg<K>& key, size_t hashval) const {
        return const_cast<parallel_hash_set*>(this)->find(key, hashval);
    }

    template <class K = key_type>
    const_iterator find(const key_arg<K>& key) const {
        return find(key, this->hash(key));
    }

    template <class K = key_type>
    bool contains(const key_arg<K>& key) const {
        return find(key) != end();
    }

    template <class K = key_type>
    bool contains(const key_arg<K>& key, size_t hashval) const {
        return find(key, hashval) != end();
    }

    template <class K = key_type>
    std::pair<iterator, iterator> equal_range(const key_arg<K>& key) {
        auto it = find(key);
        if (it != end()) return {it, std::next(it)};
        return {it, it};
    }

    template <class K = key_type>
    std::pair<const_iterator, const_iterator> equal_range(const key_arg<K>& key) const {
        auto it = find(key);
        if (it != end()) return {it, std::next(it)};
        return {it, it};
    }

    size_t bucket_count() const {
        size_t sz = 0;
        for (const auto& inner : sets_) {
            typename Lockable::SharedLock m(const_cast<Inner&>(inner));
            sz += inner.set_.bucket_count();
        }
        return sz;
    }

    float load_factor() const {
        size_t _capacity = bucket_count();
        return _capacity ? static_cast<float>(static_cast<double>(size()) / _capacity) : 0;
    }

    float max_load_factor() const { return 1.0f; }
    void max_load_factor(float) {
        // Does nothing.
    }

    hasher hash_function() const {
        return hash_ref();
    } // warning: doesn't match internal hash - use hash() member function
    key_equal key_eq() const { return eq_ref(); }
    allocator_type get_allocator() const { return alloc_ref(); }

    friend bool operator==(const parallel_hash_set& a, const parallel_hash_set& b) {
        return std::equal(a.sets_.begin(), a.sets_.end(), b.sets_.begin());
    }

    friend bool operator!=(const parallel_hash_set& a, const parallel_hash_set& b) { return !(a == b); }

    friend void swap(parallel_hash_set& a, parallel_hash_set& b) noexcept(noexcept(a.swap(b))) { a.swap(b); }

    template <class K>
    size_t hash(const K& key) const {
        return HashElement{hash_ref()}(key);
    }

#ifndef PHMAP_NON_DETERMINISTIC
    size_t dump_bound() const;

    template <typename OutputArchive>
    bool dump(OutputArchive& ar) const;

    template <typename InputArchive>
    bool load(InputArchive& ar);
#endif

private:
    template <class Container, typename Enabler>
    friend struct phmap::priv::hashtable_debug_internal::HashtableDebugAccess;

    struct FindElement {
        template <class K, class... Args>
        const_iterator operator()(const K& key, Args&&...) const {
            return s.find(key);
        }
        const parallel_hash_set& s;
    };

    struct HashElement {
        template <class K, class... Args>
        size_t operator()(const K& key, Args&&...) const {
            return phmap_mix<sizeof(size_t)>()(h(key));
        }
        const hasher& h;
    };

    template <class K1>
    struct EqualElement {
        template <class K2, class... Args>
        bool operator()(const K2& lhs, Args&&...) const {
            return eq(lhs, rhs);
        }
        const K1& rhs;
        const key_equal& eq;
    };

    // "erases" the object from the container, except that it doesn't actually
    // destroy the object. It only updates all the metadata of the class.
    // This can be used in conjunction with Policy::transfer to move the object to
    // another place.
    // --------------------------------------------------------------------
    void erase_meta_only(const_iterator cit) {
        auto& it = cit.iter_;
        assert(it.set_ != nullptr);
        it.set_.erase_meta_only(const_iterator(it.it_));
    }

    void drop_deletes_without_resize() PHMAP_ATTRIBUTE_NOINLINE {
        for (auto& inner : sets_) {
            typename Lockable::UniqueLock m(inner);
            inner.set_.drop_deletes_without_resize();
        }
    }

    bool has_element(const value_type& elem) const {
        size_t hashval = PolicyTraits::apply(HashElement{hash_ref()}, elem);
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        typename Lockable::SharedLock m(const_cast<Inner&>(inner));
        return set.has_element(elem, hashval);
    }

    // TODO(alkis): Optimize this assuming *this and that don't overlap.
    // --------------------------------------------------------------------
    parallel_hash_set& move_assign(parallel_hash_set&& that, std::true_type) {
        parallel_hash_set tmp(std::move(that));
        swap(tmp);
        return *this;
    }

    parallel_hash_set& move_assign(parallel_hash_set&& that, std::false_type) {
        parallel_hash_set tmp(std::move(that), alloc_ref());
        swap(tmp);
        return *this;
    }

protected:
    template <class K = key_type, class L = typename Lockable::SharedLock>
    pointer find_ptr(const key_arg<K>& key, size_t hashval, L& mutexlock) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        mutexlock = std::move(L(inner));
        return set.find_ptr(key, hashval);
    }

    template <class K = key_type, class L = typename Lockable::SharedLock>
    iterator find(const key_arg<K>& key, size_t hashval, L& mutexlock) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        mutexlock = std::move(L(inner));
        return make_iterator(&inner, set.find(key, hashval));
    }

    template <class K>
    std::tuple<Inner*, size_t, bool> find_or_prepare_insert_with_hash(size_t hashval, const K& key,
                                                                      typename Lockable::UniqueLock& mutexlock) {
        Inner& inner = sets_[subidx(hashval)];
        auto& set = inner.set_;
        mutexlock = std::move(typename Lockable::UniqueLock(inner));
        auto p = set.find_or_prepare_insert(key, hashval); // std::pair<size_t, bool>
        return std::make_tuple(&inner, p.first, p.second);
    }

    template <class K>
    std::tuple<Inner*, size_t, bool> find_or_prepare_insert(const K& key, typename Lockable::UniqueLock& mutexlock) {
        return find_or_prepare_insert_with_hash<K>(this->hash(key), key, mutexlock);
    }

    iterator iterator_at(Inner* inner, const EmbeddedIterator& it) { return {inner, &sets_[0] + num_tables, it}; }
    const_iterator iterator_at(Inner* inner, const EmbeddedIterator& it) const {
        return {inner, &sets_[0] + num_tables, it};
    }

    static size_t subidx(size_t hashval) {
        if constexpr (balance) {
            return ((hashval >> 8) ^ (hashval >> 16) ^ (hashval >> 24)) & mask;
        } else {
            static_assert(num_tables == 16, "unbalanced hash_set only support 16 sub-tables");
            constexpr static uint8_t idxmapping[256] = {
                    0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  2,  2,  2,  2,  2,  2,  2,
                    2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
                    4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,
                    6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,
                    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
                    9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                    10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12,
                    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13,
                    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 14, 14, 14, 14, 14, 14, 14,
                    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15,
                    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};
            return idxmapping[(((hashval >> 8) ^ (hashval >> 16) ^ (hashval >> 24)) & 0xff)];
        }
    }

    static size_t subcnt() { return num_tables; }

private:
    friend struct RawHashSetTestOnlyAccess;

    size_t growth_left() {
        size_t sz = 0;
        for (const auto& set : sets_) sz += set.growth_left();
        return sz;
    }

    hasher& hash_ref() { return sets_[0].set_.hash_ref(); }
    const hasher& hash_ref() const { return sets_[0].set_.hash_ref(); }
    key_equal& eq_ref() { return sets_[0].set_.eq_ref(); }
    const key_equal& eq_ref() const { return sets_[0].set_.eq_ref(); }
    allocator_type& alloc_ref() { return sets_[0].set_.alloc_ref(); }
    const allocator_type& alloc_ref() const { return sets_[0].set_.alloc_ref(); }

protected: // protected in case users want to derive fromm this
    std::array<Inner, num_tables> sets_;
};

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <size_t N, template <class, class, class, class> class RefSet, class Mtx_, class Policy, class Hash, class Eq,
          class Alloc, bool balance = true>
class parallel_hash_map : public parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc, balance> {
    // P is Policy. It's passed as a template argument to support maps that have
    // incomplete types as values, as in unordered_map<K, IncompleteType>.
    // MappedReference<> may be a non-reference type.
    template <class P>
    using MappedReference = decltype(P::value(std::addressof(std::declval<typename parallel_hash_map::reference>())));

    // MappedConstReference<> may be a non-reference type.
    template <class P>
    using MappedConstReference =
            decltype(P::value(std::addressof(std::declval<typename parallel_hash_map::const_reference>())));

    using KeyArgImpl = KeyArg<IsTransparent<Eq>::value && IsTransparent<Hash>::value>;

    using Base = typename parallel_hash_map::parallel_hash_set;
    using Lockable = phmap::LockableImpl<Mtx_>;

public:
    using key_type = typename Policy::key_type;
    using mapped_type = typename Policy::mapped_type;
    template <class K>
    using key_arg = typename KeyArgImpl::template type<K, key_type>;

    static_assert(!std::is_reference<key_type>::value);
    // TODO(alkis): remove this assertion and verify that reference mapped_type is
    // supported.
    static_assert(!std::is_reference<mapped_type>::value);

    using iterator = typename parallel_hash_map::parallel_hash_set::iterator;
    using const_iterator = typename parallel_hash_map::parallel_hash_set::const_iterator;

    parallel_hash_map() = default;

#ifdef __INTEL_COMPILER
    using Base::parallel_hash_set;
#else
    using parallel_hash_map::parallel_hash_set::parallel_hash_set;
#endif

    // The last two template parameters ensure that both arguments are rvalues
    // (lvalue arguments are handled by the overloads below). This is necessary
    // for supporting bitfield arguments.
    //
    //   union { int n : 1; };
    //   flat_hash_map<int, int> m;
    //   m.insert_or_assign(n, n);
    template <class K = key_type, class V = mapped_type, K* = nullptr, V* = nullptr>
    std::pair<iterator, bool> insert_or_assign(key_arg<K>&& k, V&& v) {
        return insert_or_assign_impl(std::move(k), std::forward<V>(v));
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr>
    std::pair<iterator, bool> insert_or_assign(key_arg<K>&& k, const V& v) {
        return insert_or_assign_impl(std::move(k), v);
    }

    template <class K = key_type, class V = mapped_type, V* = nullptr>
    std::pair<iterator, bool> insert_or_assign(const key_arg<K>& k, V&& v) {
        return insert_or_assign_impl(k, std::forward<V>(v));
    }

    template <class K = key_type, class V = mapped_type>
    std::pair<iterator, bool> insert_or_assign(const key_arg<K>& k, const V& v) {
        return insert_or_assign_impl(k, v);
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr, V* = nullptr>
    iterator insert_or_assign(const_iterator, key_arg<K>&& k, V&& v) {
        return insert_or_assign(std::move(k), std::forward<V>(v)).first;
    }

    template <class K = key_type, class V = mapped_type, K* = nullptr>
    iterator insert_or_assign(const_iterator, key_arg<K>&& k, const V& v) {
        return insert_or_assign(std::move(k), v).first;
    }

    template <class K = key_type, class V = mapped_type, V* = nullptr>
    iterator insert_or_assign(const_iterator, const key_arg<K>& k, V&& v) {
        return insert_or_assign(k, std::forward<V>(v)).first;
    }

    template <class K = key_type, class V = mapped_type>
    iterator insert_or_assign(const_iterator, const key_arg<K>& k, const V& v) {
        return insert_or_assign(k, v).first;
    }

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0, K* = nullptr>
    std::pair<iterator, bool> try_emplace(key_arg<K>&& k, Args&&... args) {
        return try_emplace_impl(std::move(k), std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0>
    std::pair<iterator, bool> try_emplace(const key_arg<K>& k, Args&&... args) {
        return try_emplace_impl(k, std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args, K* = nullptr>
    iterator try_emplace(const_iterator, key_arg<K>&& k, Args&&... args) {
        return try_emplace(std::move(k), std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class... Args>
    iterator try_emplace(const_iterator, const key_arg<K>& k, Args&&... args) {
        return try_emplace(k, std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class P = Policy>
    MappedReference<P> at(const key_arg<K>& key) {
        auto it = this->find(key);
        if (it == this->end()) phmap::base_internal::ThrowStdOutOfRange("phmap at(): lookup non-existent key");
        return Policy::value(&*it);
    }

    template <class K = key_type, class P = Policy>
    MappedConstReference<P> at(const key_arg<K>& key) const {
        auto it = this->find(key);
        if (it == this->end()) phmap::base_internal::ThrowStdOutOfRange("phmap at(): lookup non-existent key");
        return Policy::value(&*it);
    }

    // ----------- phmap extensions --------------------------

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0, K* = nullptr>
    std::pair<iterator, bool> try_emplace_with_hash(size_t hashval, key_arg<K>&& k, Args&&... args) {
        return try_emplace_impl_with_hash(hashval, std::move(k), std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args,
              typename std::enable_if<!std::is_convertible<K, const_iterator>::value, int>::type = 0>
    std::pair<iterator, bool> try_emplace_with_hash(size_t hashval, const key_arg<K>& k, Args&&... args) {
        return try_emplace_impl_with_hash(hashval, k, std::forward<Args>(args)...);
    }

    template <class K = key_type, class... Args, K* = nullptr>
    iterator try_emplace_with_hash(size_t hashval, const_iterator, key_arg<K>&& k, Args&&... args) {
        return try_emplace_with_hash(hashval, std::move(k), std::forward<Args>(args)...).first;
    }

    template <class K = key_type, class... Args>
    iterator try_emplace_with_hash(size_t hashval, const_iterator, const key_arg<K>& k, Args&&... args) {
        return try_emplace_with_hash(hashval, k, std::forward<Args>(args)...).first;
    }

    // if map contains key, lambda is called with the mapped value (under read lock protection),
    // and if_contains returns true. This is a const API and lambda should not modify the value
    // -----------------------------------------------------------------------------------------
    template <class K = key_type, class F>
    bool if_contains(const key_arg<K>& key, F&& f) const {
        return const_cast<parallel_hash_map*>(this)->template modify_if_impl<K, F, typename Lockable::SharedLock>(
                key, std::forward<F>(f));
    }

    // if map contains key, lambda is called with the mapped value without read lock protection,
    // and if_contains_unsafe returns true. This is a const API and lambda should not modify the value
    // This should be used only if we know that no other thread may be mutating the map at the time.
    // -----------------------------------------------------------------------------------------
    template <class K = key_type, class F>
    bool if_contains_unsafe(const key_arg<K>& key, F&& f) const {
        return const_cast<parallel_hash_map*>(this)
                ->template modify_if_impl<K, F, LockableBaseImpl<phmap::NullMutex>::DoNothing>(key, std::forward<F>(f));
    }

    // if map contains key, lambda is called with the mapped value  (under write lock protection),
    // and modify_if returns true. This is a non-const API and lambda is allowed to modify the mapped value
    // ----------------------------------------------------------------------------------------------------
    template <class K = key_type, class F>
    bool modify_if(const key_arg<K>& key, F&& f) {
        return modify_if_impl<K, F, typename Lockable::UniqueLock>(key, std::forward<F>(f));
    }

    // if map contains key, lambda is called with the mapped value  (under write lock protection).
    // If the lambda returns true, the key is subsequently erased from the map (the write lock
    // is only released after erase).
    // returns true if key was erased, false otherwise.
    // ----------------------------------------------------------------------------------------------------
    template <class K = key_type, class F>
    bool erase_if(const key_arg<K>& key, F&& f) {
        return erase_if_impl<K, F, typename Lockable::UniqueLock>(key, std::forward<F>(f));
    }

    // if map does not contains key, it is inserted and the mapped value is value-constructed
    // with the provided arguments (if any), as with try_emplace.
    // if map already  contains key, then the lambda is called with the mapped value (under
    // write lock protection) and can update the mapped value.
    // returns true if key was not already present, false otherwise.
    // ---------------------------------------------------------------------------------------
    template <class K = key_type, class F, class... Args>
    bool try_emplace_l(K&& k, F&& f, Args&&... args) {
        typename Lockable::UniqueLock m;
        auto res = this->find_or_prepare_insert(k, m);
        typename Base::Inner* inner = std::get<0>(res);
        if (std::get<2>(res))
            inner->set_.emplace_at(std::get<1>(res), std::piecewise_construct,
                                   std::forward_as_tuple(std::forward<K>(k)),
                                   std::forward_as_tuple(std::forward<Args>(args)...));
        else {
            auto it = this->iterator_at(inner, inner->set_.iterator_at(std::get<1>(res)));
            std::forward<F>(f)(Policy::value(&*it));
        }
        return std::get<2>(res);
    }

    // ----------- end of phmap extensions --------------------------

    template <class K = key_type, class P = Policy, K* = nullptr>
    MappedReference<P> operator[](key_arg<K>&& key) {
        return Policy::value(&*try_emplace(std::move(key)).first);
    }

    template <class K = key_type, class P = Policy>
    MappedReference<P> operator[](const key_arg<K>& key) {
        return Policy::value(&*try_emplace(key).first);
    }

private:
    template <class K = key_type, class F, class L>
    bool modify_if_impl(const key_arg<K>& key, F&& f) {
#if __cplusplus >= 201703L
        static_assert(std::is_invocable<F, mapped_type&>::value);
#endif
        L m;
        auto ptr = this->template find_ptr<K, L>(key, this->hash(key), m);
        if (ptr == nullptr) return false;
        std::forward<F>(f)(Policy::value(ptr));
        return true;
    }

    template <class K = key_type, class F, class L>
    bool erase_if_impl(const key_arg<K>& key, F&& f) {
#if __cplusplus >= 201703L
        static_assert(std::is_invocable<F, mapped_type&>::value);
#endif
        L m;
        auto it = this->template find<K, L>(key, this->hash(key), m);
        if (it == this->end()) return false;
        if (std::forward<F>(f)(Policy::value(&*it))) {
            this->erase(it);
            return true;
        }
        return false;
    }

    template <class K, class V>
    std::pair<iterator, bool> insert_or_assign_impl(K&& k, V&& v) {
        typename Lockable::UniqueLock m;
        auto res = this->find_or_prepare_insert(k, m);
        typename Base::Inner* inner = std::get<0>(res);
        if (std::get<2>(res))
            inner->set_.emplace_at(std::get<1>(res), std::forward<K>(k), std::forward<V>(v));
        else
            Policy::value(&*inner->set_.iterator_at(std::get<1>(res))) = std::forward<V>(v);
        return {this->iterator_at(inner, inner->set_.iterator_at(std::get<1>(res))), std::get<2>(res)};
    }

    template <class K = key_type, class... Args>
    std::pair<iterator, bool> try_emplace_impl(K&& k, Args&&... args) {
        typename Lockable::UniqueLock m;
        auto res = this->find_or_prepare_insert(k, m);
        typename Base::Inner* inner = std::get<0>(res);
        if (std::get<2>(res))
            inner->set_.emplace_at(std::get<1>(res), std::piecewise_construct,
                                   std::forward_as_tuple(std::forward<K>(k)),
                                   std::forward_as_tuple(std::forward<Args>(args)...));
        return {this->iterator_at(inner, inner->set_.iterator_at(std::get<1>(res))), std::get<2>(res)};
    }

    template <class K = key_type, class... Args>
    std::pair<iterator, bool> try_emplace_impl_with_hash(size_t hashval, K&& k, Args&&... args) {
        typename Lockable::UniqueLock m;
        auto res = this->find_or_prepare_insert_with_hash(hashval, k, m);
        typename Base::Inner* inner = std::get<0>(res);
        if (std::get<2>(res))
            inner->set_.emplace_at(std::get<1>(res), std::piecewise_construct,
                                   std::forward_as_tuple(std::forward<K>(k)),
                                   std::forward_as_tuple(std::forward<Args>(args)...));
        return {this->iterator_at(inner, inner->set_.iterator_at(std::get<1>(res))), std::get<2>(res)};
    }
};

// Constructs T into uninitialized storage pointed by `ptr` using the args
// specified in the tuple.
// ----------------------------------------------------------------------------
template <class Alloc, class T, class Tuple>
void ConstructFromTuple(Alloc* alloc, T* ptr, Tuple&& t) {
    memory_internal::ConstructFromTupleImpl(
            alloc, ptr, std::forward<Tuple>(t),
            phmap::make_index_sequence<std::tuple_size<typename std::decay<Tuple>::type>::value>());
}

// Constructs T using the args specified in the tuple and calls F with the
// constructed value.
// ----------------------------------------------------------------------------
template <class T, class Tuple, class F>
decltype(std::declval<F>()(std::declval<T>())) WithConstructed(Tuple&& t, F&& f) {
    return memory_internal::WithConstructedImpl<T>(
            std::forward<Tuple>(t),
            phmap::make_index_sequence<std::tuple_size<typename std::decay<Tuple>::type>::value>(), std::forward<F>(f));
}

// ----------------------------------------------------------------------------
// Given arguments of an std::pair's consructor, PairArgs() returns a pair of
// tuples with references to the passed arguments. The tuples contain
// constructor arguments for the first and the second elements of the pair.
//
// The following two snippets are equivalent.
//
// 1. std::pair<F, S> p(args...);
//
// 2. auto a = PairArgs(args...);
//    std::pair<F, S> p(std::piecewise_construct,
//                      std::move(p.first), std::move(p.second));
// ----------------------------------------------------------------------------
inline std::pair<std::tuple<>, std::tuple<>> PairArgs() {
    return {};
}

template <class F, class S>
std::pair<std::tuple<F&&>, std::tuple<S&&>> PairArgs(F&& f, S&& s) {
    return {std::piecewise_construct, std::forward_as_tuple(std::forward<F>(f)),
            std::forward_as_tuple(std::forward<S>(s))};
}

template <class F, class S>
std::pair<std::tuple<const F&>, std::tuple<const S&>> PairArgs(const std::pair<F, S>& p) {
    return PairArgs(p.first, p.second);
}

template <class F, class S>
std::pair<std::tuple<F&&>, std::tuple<S&&>> PairArgs(std::pair<F, S>&& p) {
    return PairArgs(std::forward<F>(p.first), std::forward<S>(p.second));
}

template <class F, class S>
auto PairArgs(std::piecewise_construct_t, F&& f, S&& s)
        -> decltype(std::make_pair(memory_internal::TupleRef(std::forward<F>(f)),
                                   memory_internal::TupleRef(std::forward<S>(s)))) {
    return std::make_pair(memory_internal::TupleRef(std::forward<F>(f)), memory_internal::TupleRef(std::forward<S>(s)));
}

// A helper function for implementing apply() in map policies.
// ----------------------------------------------------------------------------
template <class F, class... Args>
auto DecomposePair(F&& f, Args&&... args)
        -> decltype(memory_internal::DecomposePairImpl(std::forward<F>(f), PairArgs(std::forward<Args>(args)...))) {
    return memory_internal::DecomposePairImpl(std::forward<F>(f), PairArgs(std::forward<Args>(args)...));
}

// A helper function for implementing apply() in set policies.
// ----------------------------------------------------------------------------
template <class F, class Arg>
decltype(std::declval<F>()(std::declval<const Arg&>(), std::declval<Arg>())) DecomposeValue(F&& f, Arg&& arg) {
    const auto& key = arg;
    return std::forward<F>(f)(key, std::forward<Arg>(arg));
}

// --------------------------------------------------------------------------
// Policy: a policy defines how to perform different operations on
// the slots of the hashtable (see hash_policy_traits.h for the full interface
// of policy).
//
// Hash: a (possibly polymorphic) functor that hashes keys of the hashtable. The
// functor should accept a key and return size_t as hash. For best performance
// it is important that the hash function provides high entropy across all bits
// of the hash.
//
// Eq: a (possibly polymorphic) functor that compares two keys for equality. It
// should accept two (of possibly different type) keys and return a bool: true
// if they are equal, false if they are not. If two keys compare equal, then
// their hash values as defined by Hash MUST be equal.
//
// Allocator: an Allocator [https://devdocs.io/cpp/concept/allocator] with which
// the storage of the hashtable will be allocated and the elements will be
// constructed and destroyed.
// --------------------------------------------------------------------------
template <class T>
struct FlatHashSetPolicy {
    using slot_type = T;
    using key_type = T;
    using init_type = T;
    using constant_iterators = std::true_type;

    template <class Allocator, class... Args>
    static void construct(Allocator* alloc, slot_type* slot, Args&&... args) {
        phmap::allocator_traits<Allocator>::construct(*alloc, slot, std::forward<Args>(args)...);
    }

    template <class Allocator>
    static void destroy(Allocator* alloc, slot_type* slot) {
        phmap::allocator_traits<Allocator>::destroy(*alloc, slot);
    }

    template <class Allocator>
    static void transfer(Allocator* alloc, slot_type* new_slot, slot_type* old_slot) {
        construct(alloc, new_slot, std::move(*old_slot));
        destroy(alloc, old_slot);
    }

    static T& element(slot_type* slot) { return *slot; }

    template <class F, class... Args>
    static decltype(phmap::priv::DecomposeValue(std::declval<F>(), std::declval<Args>()...)) apply(F&& f,
                                                                                                   Args&&... args) {
        return phmap::priv::DecomposeValue(std::forward<F>(f), std::forward<Args>(args)...);
    }

    static size_t space_used(const T*) { return 0; }
};

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <class K, class V>
struct FlatHashMapPolicy {
    using slot_policy = priv::map_slot_policy<K, V>;
    using slot_type = typename slot_policy::slot_type;
    using key_type = K;
    using mapped_type = V;
    using init_type = std::pair</*non const*/ key_type, mapped_type>;

    template <class Allocator, class... Args>
    static void construct(Allocator* alloc, slot_type* slot, Args&&... args) {
        slot_policy::construct(alloc, slot, std::forward<Args>(args)...);
    }

    template <class Allocator>
    static void destroy(Allocator* alloc, slot_type* slot) {
        slot_policy::destroy(alloc, slot);
    }

    template <class Allocator>
    static void transfer(Allocator* alloc, slot_type* new_slot, slot_type* old_slot) {
        slot_policy::transfer(alloc, new_slot, old_slot);
    }

    template <class F, class... Args>
    static decltype(phmap::priv::DecomposePair(std::declval<F>(), std::declval<Args>()...)) apply(F&& f,
                                                                                                  Args&&... args) {
        return phmap::priv::DecomposePair(std::forward<F>(f), std::forward<Args>(args)...);
    }

    static size_t space_used(const slot_type*) { return 0; }

    static std::pair<const K, V>& element(slot_type* slot) { return slot->value; }

    static V& value(std::pair<const K, V>* kv) { return kv->second; }
    static const V& value(const std::pair<const K, V>* kv) { return kv->second; }
};

template <class Reference, class Policy>
struct node_hash_policy {
    static_assert(std::is_lvalue_reference<Reference>::value);

    using slot_type = typename std::remove_cv<typename std::remove_reference<Reference>::type>::type*;

    template <class Alloc, class... Args>
    static void construct(Alloc* alloc, slot_type* slot, Args&&... args) {
        *slot = Policy::new_element(alloc, std::forward<Args>(args)...);
    }

    template <class Alloc>
    static void destroy(Alloc* alloc, slot_type* slot) {
        Policy::delete_element(alloc, *slot);
    }

    template <class Alloc>
    static void transfer(Alloc*, slot_type* new_slot, slot_type* old_slot) {
        *new_slot = *old_slot;
    }

    static size_t space_used(const slot_type* slot) {
        if (slot == nullptr) return Policy::element_space_used(nullptr);
        return Policy::element_space_used(*slot);
    }

    static Reference element(slot_type* slot) { return **slot; }

    template <class T, class P = Policy>
    static auto value(T* elem) -> decltype(P::value(elem)) {
        return P::value(elem);
    }

    template <class... Ts, class P = Policy>
    static auto apply(Ts&&... ts) -> decltype(P::apply(std::forward<Ts>(ts)...)) {
        return P::apply(std::forward<Ts>(ts)...);
    }
};

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <class T>
struct NodeHashSetPolicy : phmap::priv::node_hash_policy<T&, NodeHashSetPolicy<T>> {
    using key_type = T;
    using init_type = T;
    using constant_iterators = std::true_type;

    template <class Allocator, class... Args>
    static T* new_element(Allocator* alloc, Args&&... args) {
        using ValueAlloc = typename phmap::allocator_traits<Allocator>::template rebind_alloc<T>;
        ValueAlloc value_alloc(*alloc);
        T* res = phmap::allocator_traits<ValueAlloc>::allocate(value_alloc, 1);
        phmap::allocator_traits<ValueAlloc>::construct(value_alloc, res, std::forward<Args>(args)...);
        return res;
    }

    template <class Allocator>
    static void delete_element(Allocator* alloc, T* elem) {
        using ValueAlloc = typename phmap::allocator_traits<Allocator>::template rebind_alloc<T>;
        ValueAlloc value_alloc(*alloc);
        phmap::allocator_traits<ValueAlloc>::destroy(value_alloc, elem);
        phmap::allocator_traits<ValueAlloc>::deallocate(value_alloc, elem, 1);
    }

    template <class F, class... Args>
    static decltype(phmap::priv::DecomposeValue(std::declval<F>(), std::declval<Args>()...)) apply(F&& f,
                                                                                                   Args&&... args) {
        return phmap::priv::DecomposeValue(std::forward<F>(f), std::forward<Args>(args)...);
    }

    static size_t element_space_used(const T*) { return sizeof(T); }
};

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <class Key, class Value>
class NodeHashMapPolicy
        : public phmap::priv::node_hash_policy<std::pair<const Key, Value>&, NodeHashMapPolicy<Key, Value>> {
    using value_type = std::pair<const Key, Value>;

public:
    using key_type = Key;
    using mapped_type = Value;
    using init_type = std::pair</*non const*/ key_type, mapped_type>;

    template <class Allocator, class... Args>
    static value_type* new_element(Allocator* alloc, Args&&... args) {
        using PairAlloc = typename phmap::allocator_traits<Allocator>::template rebind_alloc<value_type>;
        PairAlloc pair_alloc(*alloc);
        value_type* res = phmap::allocator_traits<PairAlloc>::allocate(pair_alloc, 1);
        phmap::allocator_traits<PairAlloc>::construct(pair_alloc, res, std::forward<Args>(args)...);
        return res;
    }

    template <class Allocator>
    static void delete_element(Allocator* alloc, value_type* pair) {
        using PairAlloc = typename phmap::allocator_traits<Allocator>::template rebind_alloc<value_type>;
        PairAlloc pair_alloc(*alloc);
        phmap::allocator_traits<PairAlloc>::destroy(pair_alloc, pair);
        phmap::allocator_traits<PairAlloc>::deallocate(pair_alloc, pair, 1);
    }

    template <class F, class... Args>
    static decltype(phmap::priv::DecomposePair(std::declval<F>(), std::declval<Args>()...)) apply(F&& f,
                                                                                                  Args&&... args) {
        return phmap::priv::DecomposePair(std::forward<F>(f), std::forward<Args>(args)...);
    }

    static size_t element_space_used(const value_type*) { return sizeof(value_type); }

    static Value& value(value_type* elem) { return elem->second; }
    static const Value& value(const value_type* elem) { return elem->second; }
};

namespace hashtable_debug_internal {

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------
template <typename Set>
struct HashtableDebugAccess<Set, phmap::void_t<typename Set::raw_hash_set>> {
    using Traits = typename Set::PolicyTraits;
    using Slot = typename Traits::slot_type;

    static size_t GetNumProbes(const Set& set, const typename Set::key_type& key) {
        size_t num_probes = 0;
        size_t hashval = set.hash(key);
        auto seq = set.probe(hashval);
        while (true) {
            priv::Group g{set.ctrl_ + seq.offset()};
            for (int i : g.Match(priv::H2(hashval))) {
                if (Traits::apply(typename Set::template EqualElement<typename Set::key_type>{key, set.eq_ref()},
                                  Traits::element(set.slots_ + seq.offset((size_t)i))))
                    return num_probes;
                ++num_probes;
            }
            if (g.MatchEmpty()) return num_probes;
            seq.next();
            ++num_probes;
        }
    }

    static size_t AllocatedByteSize(const Set& c) {
        size_t capacity = c.capacity_;
        if (capacity == 0) return 0;
        auto layout = Set::MakeLayout(capacity);
        size_t m = layout.AllocSize();

        size_t per_slot = Traits::space_used(static_cast<const Slot*>(nullptr));
        if (per_slot != ~size_t{}) {
            m += per_slot * c.size();
        } else {
            for (size_t i = 0; i != capacity; ++i) {
                if (priv::IsFull(c.ctrl_[i])) {
                    m += Traits::space_used(c.slots_ + i);
                }
            }
        }
        return m;
    }

    static size_t LowerBoundAllocatedByteSize(size_t size) {
        size_t capacity = GrowthToLowerboundCapacity(size);
        if (capacity == 0) return 0;
        auto layout = Set::MakeLayout(NormalizeCapacity(capacity));
        size_t m = layout.AllocSize();
        size_t per_slot = Traits::space_used(static_cast<const Slot*>(nullptr));
        if (per_slot != ~size_t{}) {
            m += per_slot * size;
        }
        return m;
    }
};

} // namespace hashtable_debug_internal
} // namespace priv

// -----------------------------------------------------------------------------
// phmap::flat_hash_set
// -----------------------------------------------------------------------------
// An `phmap::flat_hash_set<T>` is an unordered associative container which has
// been optimized for both speed and memory footprint in most common use cases.
// Its interface is similar to that of `std::unordered_set<T>` with the
// following notable differences:
//
// * Supports heterogeneous lookup, through `find()`, `operator[]()` and
//   `insert()`, provided that the set is provided a compatible heterogeneous
//   hashing function and equality operator.
// * Invalidates any references and pointers to elements within the table after
//   `rehash()`.
// * Contains a `capacity()` member function indicating the number of element
//   slots (open, deleted, and empty) within the hash set.
// * Returns `void` from the `_erase(iterator)` overload.
// -----------------------------------------------------------------------------
template <class T, class Hash, class Eq, class Alloc> // default values in phmap_fwd_decl.h
class flat_hash_set : public phmap::priv::raw_hash_set<phmap::priv::FlatHashSetPolicy<T>, Hash, Eq, Alloc> {
    using Base = typename flat_hash_set::raw_hash_set;

public:
    flat_hash_set() = default;
#ifdef __INTEL_COMPILER
    using Base::raw_hash_set;
#else
    using Base::Base;
#endif
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear; // may shrink - To avoid shrinking `erase(begin(), end())`
    using Base::erase;
    using Base::erase_with_hash;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::emplace_with_hash;
    using Base::emplace_hint_with_hash;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;
};

// -----------------------------------------------------------------------------
// phmap::flat_hash_map
// -----------------------------------------------------------------------------
//
// An `phmap::flat_hash_map<K, V>` is an unordered associative container which
// has been optimized for both speed and memory footprint in most common use
// cases. Its interface is similar to that of `std::unordered_map<K, V>` with
// the following notable differences:
//
// * Supports heterogeneous lookup, through `find()`, `operator[]()` and
//   `insert()`, provided that the map is provided a compatible heterogeneous
//   hashing function and equality operator.
// * Invalidates any references and pointers to elements within the table after
//   `rehash()`.
// * Contains a `capacity()` member function indicating the number of element
//   slots (open, deleted, and empty) within the hash map.
// * Returns `void` from the `_erase(iterator)` overload.
// -----------------------------------------------------------------------------
template <class K, class V, class Hash, class Eq, class Alloc> // default values in phmap_fwd_decl.h
class flat_hash_map : public phmap::priv::raw_hash_map<phmap::priv::FlatHashMapPolicy<K, V>, Hash, Eq, Alloc> {
    using Base = typename flat_hash_map::raw_hash_map;

public:
    flat_hash_map() = default;
#ifdef __INTEL_COMPILER
    using Base::raw_hash_map;
#else
    using Base::Base;
#endif
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;
};

// -----------------------------------------------------------------------------
// phmap::node_hash_set
// -----------------------------------------------------------------------------
// An `phmap::node_hash_set<T>` is an unordered associative container which
// has been optimized for both speed and memory footprint in most common use
// cases. Its interface is similar to that of `std::unordered_set<T>` with the
// following notable differences:
//
// * Supports heterogeneous lookup, through `find()`, `operator[]()` and
//   `insert()`, provided that the map is provided a compatible heterogeneous
//   hashing function and equality operator.
// * Contains a `capacity()` member function indicating the number of element
//   slots (open, deleted, and empty) within the hash set.
// * Returns `void` from the `erase(iterator)` overload.
// -----------------------------------------------------------------------------
template <class T, class Hash, class Eq, class Alloc> // default values in phmap_fwd_decl.h
class node_hash_set : public phmap::priv::raw_hash_set<phmap::priv::NodeHashSetPolicy<T>, Hash, Eq, Alloc> {
    using Base = typename node_hash_set::raw_hash_set;

public:
    node_hash_set() = default;
#ifdef __INTEL_COMPILER
    using Base::raw_hash_set;
#else
    using Base::Base;
#endif
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;
    typename Base::hasher hash_funct() { return this->hash_function(); }
    void resize(typename Base::size_type hint) { this->rehash(hint); }
};

// -----------------------------------------------------------------------------
// phmap::node_hash_map
// -----------------------------------------------------------------------------
//
// An `phmap::node_hash_map<K, V>` is an unordered associative container which
// has been optimized for both speed and memory footprint in most common use
// cases. Its interface is similar to that of `std::unordered_map<K, V>` with
// the following notable differences:
//
// * Supports heterogeneous lookup, through `find()`, `operator[]()` and
//   `insert()`, provided that the map is provided a compatible heterogeneous
//   hashing function and equality operator.
// * Contains a `capacity()` member function indicating the number of element
//   slots (open, deleted, and empty) within the hash map.
// * Returns `void` from the `erase(iterator)` overload.
// -----------------------------------------------------------------------------
template <class Key, class Value, class Hash, class Eq,
          class Alloc> // default values in phmap_fwd_decl.h
class node_hash_map : public phmap::priv::raw_hash_map<phmap::priv::NodeHashMapPolicy<Key, Value>, Hash, Eq, Alloc> {
    using Base = typename node_hash_map::raw_hash_map;

public:
    node_hash_map() = default;
#ifdef __INTEL_COMPILER
    using Base::raw_hash_map;
#else
    using Base::Base;
#endif
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;
    typename Base::hasher hash_funct() { return this->hash_function(); }
    void resize(typename Base::size_type hint) { this->rehash(hint); }
};

// -----------------------------------------------------------------------------
// phmap::parallel_flat_hash_set
// -----------------------------------------------------------------------------
template <class T, class Hash, class Eq, class Alloc, size_t N, class Mtx_,
          bool balance> // default values in phmap_fwd_decl.h
class parallel_flat_hash_set
        : public phmap::priv::parallel_hash_set<N, phmap::priv::raw_hash_set, Mtx_, phmap::priv::FlatHashSetPolicy<T>,
                                                Hash, Eq, Alloc, balance> {
    using Base = typename parallel_flat_hash_set::parallel_hash_set;

public:
    parallel_flat_hash_set() = default;
#ifdef __INTEL_COMPILER
    using Base::parallel_hash_set;
#else
    using Base::Base;
#endif
    using Base::hash;
    using Base::subidx;
    using Base::subcnt;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::erase_with_hash;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::emplace_with_hash;
    using Base::emplace_hint_with_hash;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::key_eq;
};

// -----------------------------------------------------------------------------
// phmap::parallel_flat_hash_map - default values in phmap_fwd_decl.h
// -----------------------------------------------------------------------------
template <class K, class V, class Hash, class Eq, class Alloc, size_t N, class Mtx_, bool balance>
class parallel_flat_hash_map
        : public phmap::priv::parallel_hash_map<N, phmap::priv::raw_hash_set, Mtx_,
                                                phmap::priv::FlatHashMapPolicy<K, V>, Hash, Eq, Alloc, balance> {
    using Base = typename parallel_flat_hash_map::parallel_hash_map;

public:
    parallel_flat_hash_map() = default;
#ifdef __INTEL_COMPILER
    using Base::parallel_hash_map;
#else
    using Base::Base;
#endif
    using Base::hash;
    using Base::subidx;
    using Base::subcnt;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::emplace_with_hash;
    using Base::emplace_hint_with_hash;
    using Base::try_emplace_with_hash;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::key_eq;
};

// -----------------------------------------------------------------------------
// phmap::parallel_node_hash_set
// -----------------------------------------------------------------------------
template <class T, class Hash, class Eq, class Alloc, size_t N, class Mtx_>
class parallel_node_hash_set
        : public phmap::priv::parallel_hash_set<N, phmap::priv::raw_hash_set, Mtx_, phmap::priv::NodeHashSetPolicy<T>,
                                                Hash, Eq, Alloc> {
    using Base = typename parallel_node_hash_set::parallel_hash_set;

public:
    parallel_node_hash_set() = default;
#ifdef __INTEL_COMPILER
    using Base::parallel_hash_set;
#else
    using Base::Base;
#endif
    using Base::hash;
    using Base::subidx;
    using Base::subcnt;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::emplace_with_hash;
    using Base::emplace_hint_with_hash;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::key_eq;
    typename Base::hasher hash_funct() { return this->hash_function(); }
    void resize(typename Base::size_type hint) { this->rehash(hint); }
};

// -----------------------------------------------------------------------------
// phmap::parallel_node_hash_map
// -----------------------------------------------------------------------------
template <class Key, class Value, class Hash, class Eq, class Alloc, size_t N, class Mtx_>
class parallel_node_hash_map
        : public phmap::priv::parallel_hash_map<N, phmap::priv::raw_hash_set, Mtx_,
                                                phmap::priv::NodeHashMapPolicy<Key, Value>, Hash, Eq, Alloc> {
    using Base = typename parallel_node_hash_map::parallel_hash_map;

public:
    parallel_node_hash_map() = default;
#ifdef __INTEL_COMPILER
    using Base::parallel_hash_map;
#else
    using Base::Base;
#endif
    using Base::hash;
    using Base::subidx;
    using Base::subcnt;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::emplace_with_hash;
    using Base::emplace_hint_with_hash;
    using Base::try_emplace_with_hash;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::key_eq;
    typename Base::hasher hash_funct() { return this->hash_function(); }
    void resize(typename Base::size_type hint) { this->rehash(hint); }
};

} // namespace phmap

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // phmap_h_guard_
