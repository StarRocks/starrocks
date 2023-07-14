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

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace starrocks::raw {

// RawAllocator allocates `trailing` more object(not bytes) than caller required,
// to avoid overflow when the memory is operated with 128-bit aligned instructions,
// such as `_mm_testc_si128`.
//
// Also it does not initiate allocated object to zero-value.
// This behavior is shipped from raw::RawAllocator.
//
// C++ Reference recommend to use this allocator implementation to
// prevent containers resize invocation from initializing the allocated
// memory space unnecessarily.
//
//   https://stackoverflow.com/questions/21028299/is-this-behavior-of-vectorresizesize-type-n-under-c11-and-boost-container/21028912#21028912
//
// Allocator adaptor that interposes construct() calls to
// convert value initialization into default initialization.
template <typename T, size_t trailing = 16, typename A = std::allocator<T>>
class RawAllocator : public A {
    static_assert(std::is_trivially_destructible_v<T>, "not trivially destructible type");
    typedef std::allocator_traits<A> a_t;

public:
    template <typename U>
    struct rebind {
        using other = RawAllocator<U, trailing, typename a_t::template rebind_alloc<U>>;
    };

    using A::A;

    // allocate more than caller required
    constexpr T* allocate(size_t n) {
        T* x = A::allocate(n + RawAllocator::_trailing);
        return x;
    }

    // deallocate the storage referenced by the pointer p
    constexpr void deallocate(T* p, size_t n) { A::deallocate(p, (n + RawAllocator::_trailing)); }

    // do not initialized allocated.
    template <typename U>
    void construct(U* ptr) noexcept(std::is_nothrow_default_constructible<U>::value) {
        ::new (static_cast<void*>(ptr)) U;
    }
    template <typename U, typename... Args>
    void construct(U* ptr, Args&&... args) {
        a_t::construct(static_cast<A&>(*this), ptr, std::forward<Args>(args)...);
    }

private:
    static const size_t _trailing = trailing;
};

template <typename T, std::size_t N = 16>
class AlignmentAllocator {
public:
    typedef T value_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    typedef T* pointer;
    typedef const T* const_pointer;

    typedef T& reference;
    typedef const T& const_reference;

public:
    AlignmentAllocator() throw() = default;

    template <typename T2>
    AlignmentAllocator(const AlignmentAllocator<T2, N>&) throw() {}

    ~AlignmentAllocator() throw() = default;

    pointer adress(reference r) { return &r; }

    const_pointer adress(const_reference r) const { return &r; }

    pointer allocate(size_type n) {
        if (n * sizeof(value_type) < N) {
            return (pointer)std::aligned_alloc(N, N);
        }
        return (pointer)std::aligned_alloc(N, n * sizeof(value_type));
    }

    void deallocate(pointer p, size_type) { free(p); }

    void construct(pointer p, const value_type& wert) { new (p) value_type(wert); }

    void destroy(pointer p) { p->~value_type(); }

    size_type max_size() const throw() { return size_type(-1) / sizeof(value_type); }

    template <typename T2>
    struct rebind {
        typedef AlignmentAllocator<T2, N> other;
    };

    bool operator!=(const AlignmentAllocator<T, N>& other) const { return !(*this == other); }

    // Returns true if and only if storage allocated from *this
    // can be deallocated from other, and vice versa.
    // Always returns true for stateless allocators.
    bool operator==(const AlignmentAllocator<T, N>& other) const { return true; }
};

// https://github.com/StarRocks/starrocks/issues/233
// older versions of CXX11 string abi are cow semantic and our optimization to provide raw_string causes crashes.
//
// So we can't use this optimization when we detect a link to an old version of abi,
// and this may affect performance. So we strongly recommend to use the new version of abi
//
#if _GLIBCXX_USE_CXX11_ABI
using RawString = std::basic_string<char, std::char_traits<char>, RawAllocator<char, 0>>;
using RawStringPad16 = std::basic_string<char, std::char_traits<char>, RawAllocator<char, 16>>;
#else
using RawString = std::string;
using RawStringPad16 = std::string;
#endif
// From cpp reference: "A trivial destructor is a destructor that performs no action. Objects with
// trivial destructors don't require a delete-expression and may be disposed of by simply
// deallocating their storage. All data types compatible with the C language (POD types)
// are trivially destructible."
// Types with trivial destructors is safe when when move content from a RawVectorPad16<T> into
// a std::vector<U> and both T and U the same bit width, i.e.
// starrocks::raw::RawVectorPad16<int8_t> a;
// a.resize(100);
// std::vector<uint8_t> b = std::move(reinterpret_cast<std::vector<uint8_t>&>(a));
template <class T>
using RawVector = std::vector<T, RawAllocator<T, 0>>;

template <class T>
using RawVectorPad16 = std::vector<T, RawAllocator<T, 16>>;

template <class T>
inline void make_room(std::vector<T>* v, size_t n) {
    RawVector<T> rv;
    rv.resize(n);
    v->swap(reinterpret_cast<std::vector<T>&>(rv));
}

inline void make_room(std::string* s, size_t n) {
    RawString rs;
    rs.resize(n);
    s->swap(reinterpret_cast<std::string&>(rs));
}

template <typename T>
inline void stl_vector_resize_uninitialized(std::vector<T>* vec, size_t new_size) {
    ((RawVector<T>*)vec)->resize(new_size);
}

inline void stl_string_resize_uninitialized(std::string* str, size_t new_size) {
    ((RawString*)str)->resize(new_size);
}

} // namespace starrocks::raw
