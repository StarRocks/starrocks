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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "gutil/macros.h"
#include "gutil/strings/fastmem.h"
#include "runtime/memory/memory_allocator.h"
#include "util/stack_util.h"

namespace starrocks::util {

template <typename T>
struct is_relocatable : std::is_trivially_copyable<T> {};

template <typename T>
concept has_relocatable_tag = requires {
    typename T::is_relocatable;
};
template <has_relocatable_tag T>
struct is_relocatable<T> : T::is_relocatable {};

template <typename T>
inline constexpr bool is_relocatable_v = is_relocatable<T>::value;

static constexpr size_t empty_raw_buffer_size = 1024;
alignas(std::max_align_t) extern uint8_t empty_raw_buffer[empty_raw_buffer_size];

// Helper functions for element size calculations
inline size_t element_bytes(size_t element_num, size_t element_size) {
    size_t bytes;
    if (UNLIKELY(__builtin_mul_overflow(element_size, element_num, &bytes))) {
        throw std::runtime_error(
                fmt::format("element_bytes overflow: element_num={}, element_size={}", element_num, element_size));
    }
    return bytes;
}

inline size_t element_bytes_with_padding(size_t element_num, size_t element_size, size_t padding) {
    size_t base_bytes = element_bytes(element_num, element_size);
    size_t bytes;
    if (UNLIKELY(__builtin_add_overflow(base_bytes, padding, &bytes))) {
        throw std::runtime_error(
                fmt::format("element_bytes_with_padding overflow: element_num={}, element_size={}, "
                            "padding={}",
                            element_num, element_size, padding));
    }
    return bytes;
}

template <class T, size_t padding>
class RawBuffer {
public:
    static constexpr size_t kElementSize = sizeof(T);
    static constexpr size_t kPadding = ((padding + kElementSize - 1) / kElementSize) * kElementSize;
    static constexpr uint8_t* null = empty_raw_buffer;
    static_assert(alignof(T) <= alignof(std::max_align_t),
                  "RawBuffer only supports element types aligned to at most std::max_align_t");

    using value_type = T;
    using iterator = T*;
    using const_iterator = const T*;

    RawBuffer() = default;
    RawBuffer(RawBuffer&& rhs) noexcept;
    RawBuffer& operator=(RawBuffer&&) noexcept;
    DISALLOW_COPY(RawBuffer);
    ~RawBuffer();

    T* data() { return reinterpret_cast<T*>(_start); }
    const T* data() const { return reinterpret_cast<const T*>(_start); }
    iterator begin() { return reinterpret_cast<T*>(_start); }
    const_iterator begin() const { return reinterpret_cast<const T*>(_start); }
    iterator end() { return reinterpret_cast<T*>(_end); }
    const_iterator end() const { return reinterpret_cast<const T*>(_end); }
    bool empty() const { return _start == _end; }
    size_t size() const { return (_end - _start) / kElementSize; }
    size_t capacity() const { return (_end_of_storage - _start) / kElementSize; }
    size_t allocated_bytes() const { return _start == null ? 0 : _end_of_storage - _start + kPadding; }
    size_t used_bytes() const { return _end - _start; }

    T& operator[](size_t idx) { return data()[idx]; }
    const T& operator[](size_t idx) const { return data()[idx]; }
    const T& front() const {
        DCHECK(!empty());
        return *begin();
    }
    T& front() {
        DCHECK(!empty());
        return *begin();
    }
    const T& back() const {
        DCHECK(!empty());
        return *(end() - 1);
    }
    T& back() {
        DCHECK(!empty());
        return *(end() - 1);
    }

    void clear();
    void release(memory::Allocator* allocator);
    void reserve(memory::Allocator* allocator, size_t new_cap);
    void shrink_to_fit(memory::Allocator* allocator);
    void resize(memory::Allocator* allocator, size_t count);
    void resize(memory::Allocator* allocator, size_t count, const T& value);

    void assign(memory::Allocator* allocator, size_t count, const T& value);
    template <class InputIt,
              std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                 typename std::iterator_traits<InputIt>::iterator_category>,
                               bool> = true>
    void assign(memory::Allocator* allocator, InputIt first, InputIt last);
    void assign(memory::Allocator* allocator, std::initializer_list<T> ilist);

    void push_back(memory::Allocator* allocator, const T& value);
    void push_back(memory::Allocator* allocator, T&& value);
    template <class... Args>
    T& emplace_back(memory::Allocator* allocator, Args&&... args);
    void pop_back();

    template <class InputIt,
              std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                 typename std::iterator_traits<InputIt>::iterator_category>,
                               bool> = true>
    iterator append(memory::Allocator* allocator, InputIt first, InputIt last);
    iterator append(memory::Allocator* allocator, std::initializer_list<T> ilist);
    iterator append(memory::Allocator* allocator, size_t count, const T& value);

    void swap(RawBuffer& other) noexcept;

private:
    void relocate(memory::Allocator* allocator, size_t new_bytes);
    void grow(memory::Allocator* allocator);

    void destroy(iterator first, iterator last);

    size_t required_bytes(size_t element_num) const {
        if constexpr (padding > 0) {
            return element_bytes_with_padding(element_num, kElementSize, kPadding);
        } else {
            return element_bytes(element_num, kElementSize);
        }
    }

protected:
    uint8_t* _start = null;
    uint8_t* _end = null;
    uint8_t* _end_of_storage = null;
};

template <class T, size_t padding = 0>
class Buffer : public RawBuffer<T, padding> {
public:
    using iterator = typename RawBuffer<T, padding>::iterator;
    using const_iterator = typename RawBuffer<T, padding>::const_iterator;
    using value_type = typename RawBuffer<T, padding>::value_type;

    Buffer() = delete;
    explicit Buffer(memory::Allocator* allocator) : _allocator(allocator) {}
    Buffer(memory::Allocator* allocator, size_t count) : _allocator(allocator) { this->resize(count); }
    Buffer(memory::Allocator* allocator, size_t count, const T& value) : _allocator(allocator) {
        this->assign(count, value);
    }
    Buffer(Buffer&&) noexcept;
    Buffer& operator=(Buffer&&) noexcept;
    DISALLOW_COPY(Buffer);
    ~Buffer();

    void release();
    void reserve(size_t new_cap);
    void shrink_to_fit();
    void resize(size_t count);
    void resize(size_t count, const T& value);
    void assign(size_t count, const T& value);
    template <class InputIt,
              std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                 typename std::iterator_traits<InputIt>::iterator_category>,
                               bool> = true>
    void assign(InputIt first, InputIt last);
    void assign(std::initializer_list<T> ilist);

    void push_back(const T& value);
    void push_back(T&& value);
    template <class... Args>
    T& emplace_back(Args&&... args);
    void pop_back();

    template <class InputIt,
              std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                 typename std::iterator_traits<InputIt>::iterator_category>,
                               bool> = true>
    iterator append(InputIt first, InputIt last);
    iterator append(std::initializer_list<T> ilist);
    iterator append(size_t count, const T& value);

    void swap(Buffer& other) noexcept;

    memory::Allocator* allocator() const { return _allocator; }

private:
    memory::Allocator* _allocator = nullptr;
};

template <class T, size_t padding>
RawBuffer<T, padding>::RawBuffer(RawBuffer&& rhs) noexcept {
    std::swap(_start, rhs._start);
    std::swap(_end, rhs._end);
    std::swap(_end_of_storage, rhs._end_of_storage);
}

template <class T, size_t padding>
RawBuffer<T, padding>& RawBuffer<T, padding>::operator=(RawBuffer&& rhs) noexcept {
    if (this != &rhs) {
        std::swap(_start, rhs._start);
        std::swap(_end, rhs._end);
        std::swap(_end_of_storage, rhs._end_of_storage);
    }
    return *this;
}

template <class T, size_t padding>
RawBuffer<T, padding>::~RawBuffer() {
    DCHECK(_start == null) << "_start must be null, " << get_stack_trace();
    DCHECK(_end == null) << "_end must be null, " << get_stack_trace();
    DCHECK(_end_of_storage == null) << "_end_of_storage must be null, " << get_stack_trace();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::clear() {
    destroy(begin(), end());
    _end = _start;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::release(memory::Allocator* allocator) {
    if (_start == null) {
        return;
    }

    destroy(begin(), end());
    allocator->free(reinterpret_cast<void*>(_start), allocated_bytes());

    _start = null;
    _end = null;
    _end_of_storage = null;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::relocate(memory::Allocator* allocator, size_t new_allocated_bytes) {
    if (UNLIKELY(_start == null)) {
        _start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
        DCHECK(_start != nullptr);
        _end = _start;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
        return;
    }
    size_t old_allocated_bytes = allocated_bytes();
    size_t old_used_bytes = used_bytes();

    if constexpr (is_relocatable_v<T>) {
        if (old_allocated_bytes >= 4096) {
            uint8_t* new_start = reinterpret_cast<uint8_t*>(
                    allocator->realloc(reinterpret_cast<void*>(_start), old_allocated_bytes, new_allocated_bytes));
            DCHECK(new_start != nullptr);
            _start = new_start;
            _end = _start + old_used_bytes;
            _end_of_storage = _start + new_allocated_bytes - kPadding;
        } else {
            uint8_t* new_start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
            DCHECK(new_start != nullptr);
            strings::memcpy_inlined(new_start, reinterpret_cast<const void*>(_start), old_used_bytes);
            allocator->free(reinterpret_cast<void*>(_start), old_allocated_bytes);
            _start = new_start;
            _end = _start + old_used_bytes;
            _end_of_storage = _start + new_allocated_bytes - kPadding;
        }
    } else {
        uint8_t* new_start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
        DCHECK(new_start != nullptr);
        T* new_data = reinterpret_cast<T*>(new_start);
        T* old_data = reinterpret_cast<T*>(_start);
        size_t old_size = size();
        try {
            if constexpr (std::is_nothrow_move_constructible_v<T>) {
                std::uninitialized_move(old_data, old_data + old_size, new_data);
            } else {
                std::uninitialized_copy(old_data, old_data + old_size, new_data);
            }
        } catch (...) {
            allocator->free(reinterpret_cast<void*>(new_start), new_allocated_bytes);
            throw;
        }
        destroy(old_data, old_data + old_size);
        allocator->free(reinterpret_cast<void*>(_start), old_allocated_bytes);
        _start = new_start;
        _end = _start + old_used_bytes;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
    }
}

template <class T, size_t padding>
void RawBuffer<T, padding>::grow(memory::Allocator* allocator) {
    if (empty()) {
        size_t allocate_bytes = required_bytes(1);
        relocate(allocator, allocate_bytes);
    } else {
        relocate(allocator, allocated_bytes() * 2);
    }
}

template <class T, size_t padding>
void RawBuffer<T, padding>::destroy(iterator first, iterator last) {
    if constexpr (!std::is_trivially_destructible_v<T>) {
        std::destroy(first, last);
    }
}

template <class T, size_t padding>
void RawBuffer<T, padding>::reserve(memory::Allocator* allocator, size_t new_cap) {
    if (capacity() >= new_cap) {
        return;
    }

    size_t new_allocated_bytes = required_bytes(new_cap);

    relocate(allocator, new_allocated_bytes);
}

template <class T, size_t padding>
void RawBuffer<T, padding>::shrink_to_fit(memory::Allocator* allocator) {
    if (_end == _end_of_storage) {
        return;
    }

    size_t new_allocated_bytes = required_bytes(size());

    relocate(allocator, new_allocated_bytes);
}

template <class T, size_t padding>
void RawBuffer<T, padding>::resize(memory::Allocator* allocator, size_t new_size) {
    // For trivially copyable T, expanding size does not initialize new elements.
    if (new_size > capacity()) {
        size_t new_capacity = std::max(new_size, capacity() * 2);
        reserve(allocator, new_capacity);
    }

    size_t old_size = size();
    if (new_size < old_size) {
        destroy(data() + new_size, data() + old_size);
    } else if (new_size > old_size) {
        if constexpr (!std::is_trivially_copyable_v<T>) {
            std::uninitialized_value_construct_n(data() + old_size, new_size - old_size);
        }
    }

    _end = _start + new_size * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::resize(memory::Allocator* allocator, size_t new_size, const T& value) {
    if (new_size > capacity()) {
        size_t new_capacity = std::max(new_size, capacity() * 2);
        reserve(allocator, new_capacity);
    }

    size_t old_size = size();

    if (new_size > old_size) {
        if constexpr (std::is_trivially_copyable_v<T>) {
            std::fill(end(), end() + new_size - old_size, value);
        } else {
            std::uninitialized_fill_n(end(), new_size - old_size, value);
        }
    } else if (new_size < old_size) {
        destroy(data() + new_size, data() + old_size);
    }
    _end = _start + new_size * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, size_t count, const T& value) {
    if (count > capacity()) {
        reserve(allocator, count);
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
        std::fill(begin(), begin() + count, value);
    } else {
        destroy(begin(), end());
        _end = _start;
        std::uninitialized_fill_n(begin(), count, value);
    }
    _end = _start + count * kElementSize;
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                            typename std::iterator_traits<InputIt>::iterator_category>,
                                          bool>>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, InputIt first, InputIt last) {
    size_t count = std::distance(first, last);
    if (count > capacity()) {
        reserve(allocator, count);
    }

    destroy(begin(), end());
    _end = _start;

    T* ptr = data();
    if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<InputIt>) {
        strings::memcpy_inlined(ptr, reinterpret_cast<const void*>(first), count * kElementSize);
    } else {
        std::uninitialized_copy(first, last, ptr);
    }
    _end = _start + count * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, std::initializer_list<T> ilist) {
    assign(allocator, ilist.begin(), ilist.end());
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(memory::Allocator* allocator, const T& value) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        grow(allocator);
    }
    new (end()) T(value);
    _end += kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(memory::Allocator* allocator, T&& value) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        grow(allocator);
    }
    new (end()) T(std::move(value));
    _end += kElementSize;
}

template <class T, size_t padding>
template <class... Args>
T& RawBuffer<T, padding>::emplace_back(memory::Allocator* allocator, Args&&... args) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        grow(allocator);
    }
    T* ptr = end();
    new (ptr) T(std::forward<Args>(args)...);
    _end += kElementSize;
    return *ptr;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::pop_back() {
    if (UNLIKELY(empty())) {
        return;
    }
    _end -= kElementSize;
    destroy(end(), end() + 1);
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                            typename std::iterator_traits<InputIt>::iterator_category>,
                                          bool>>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::append(memory::Allocator* allocator, InputIt first,
                                                                        InputIt last) {
    size_t count = std::distance(first, last);
    if (UNLIKELY(count == 0)) {
        return end();
    }
    size_t required_capacity = size() + count;
    if (required_capacity > capacity()) {
        size_t new_cap = empty() ? count : std::max(capacity() * 2, required_capacity);
        reserve(allocator, new_cap);
    }

    T* insert_pos = reinterpret_cast<T*>(_end);
    // Append new elements at the end
    if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<InputIt>) {
        strings::memcpy_inlined(insert_pos, reinterpret_cast<const void*>(first), count * kElementSize);
    } else {
        std::uninitialized_copy(first, last, insert_pos);
    }
    _end += count * kElementSize;
    return insert_pos;
}

template <class T, size_t padding>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::append(memory::Allocator* allocator,
                                                                        std::initializer_list<T> ilist) {
    return append(allocator, ilist.begin(), ilist.end());
}

template <class T, size_t padding>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::append(memory::Allocator* allocator, size_t count,
                                                                        const T& value) {
    if (UNLIKELY(count == 0)) {
        return end();
    }
    size_t required_capacity = size() + count;
    if (required_capacity > capacity()) {
        size_t new_cap = empty() ? count : std::max(capacity() * 2, required_capacity);
        reserve(allocator, new_cap);
    }
    T* insert_pos = end();
    // Append new elements at the end
    if constexpr (std::is_trivially_copyable_v<T>) {
        std::fill(end(), end() + count, value);
    } else {
        std::uninitialized_fill_n(end(), count, value);
    }
    _end += count * kElementSize;
    return insert_pos;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::swap(RawBuffer& other) noexcept {
    std::swap(_start, other._start);
    std::swap(_end, other._end);
    std::swap(_end_of_storage, other._end_of_storage);
}

template <class T, size_t padding>
Buffer<T, padding>::Buffer(Buffer&& other) noexcept {
    swap(other);
}

template <class T, size_t padding>
Buffer<T, padding>::~Buffer() {
    release();
    _allocator = nullptr;
}

template <class T, size_t padding>
Buffer<T, padding>& Buffer<T, padding>::operator=(Buffer&& other) noexcept {
    swap(other);
    return *this;
}

template <class T, size_t padding>
void Buffer<T, padding>::release() {
    RawBuffer<T, padding>::release(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::reserve(size_t new_cap) {
    RawBuffer<T, padding>::reserve(this->_allocator, new_cap);
}

template <class T, size_t padding>
void Buffer<T, padding>::shrink_to_fit() {
    RawBuffer<T, padding>::shrink_to_fit(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::resize(size_t count) {
    RawBuffer<T, padding>::resize(this->_allocator, count);
}

template <class T, size_t padding>
void Buffer<T, padding>::resize(size_t count, const T& value) {
    RawBuffer<T, padding>::resize(this->_allocator, count, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(size_t count, const T& value) {
    RawBuffer<T, padding>::assign(this->_allocator, count, value);
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                            typename std::iterator_traits<InputIt>::iterator_category>,
                                          bool>>
void Buffer<T, padding>::assign(InputIt first, InputIt last) {
    RawBuffer<T, padding>::assign(this->_allocator, first, last);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(std::initializer_list<T> ilist) {
    RawBuffer<T, padding>::assign(this->_allocator, ilist);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(const T& value) {
    RawBuffer<T, padding>::push_back(this->_allocator, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(T&& value) {
    RawBuffer<T, padding>::push_back(this->_allocator, std::move(value));
}

template <class T, size_t padding>
template <class... Args>
T& Buffer<T, padding>::emplace_back(Args&&... args) {
    return RawBuffer<T, padding>::emplace_back(this->_allocator, std::forward<Args>(args)...);
}

template <class T, size_t padding>
void Buffer<T, padding>::pop_back() {
    RawBuffer<T, padding>::pop_back();
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag,
                                                            typename std::iterator_traits<InputIt>::iterator_category>,
                                          bool>>
typename Buffer<T, padding>::iterator Buffer<T, padding>::append(InputIt first, InputIt last) {
    return RawBuffer<T, padding>::append(this->_allocator, first, last);
}

template <class T, size_t padding>
typename Buffer<T, padding>::iterator Buffer<T, padding>::append(std::initializer_list<T> ilist) {
    return RawBuffer<T, padding>::append(this->_allocator, ilist);
}

template <class T, size_t padding>
typename Buffer<T, padding>::iterator Buffer<T, padding>::append(size_t count, const T& value) {
    return RawBuffer<T, padding>::append(this->_allocator, count, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::swap(Buffer& other) noexcept {
    RawBuffer<T, padding>::swap(other);
    std::swap(this->_allocator, other._allocator);
}

} // namespace starrocks::util
