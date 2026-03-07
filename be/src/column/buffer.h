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

#include "column/raw_buffer.h"

namespace starrocks::util {

/// RAII wrapper around RawBuffer that holds a memory::Allocator* and forwards all mutating
/// calls to RawBuffer with that allocator. Frees memory in the destructor via release().
/// Intended as the default Column Buffer implementation when a fixed allocator is available.
///
/// Differences from std::vector:
/// - No default constructor: must be constructed with Buffer(allocator) or
///   Buffer(allocator, count[, value]).
/// - allocator() returns the bound allocator; all allocations use it.
/// - Copy is disabled (move-only). swap() also swaps the allocator.
/// - Otherwise API matches vector (reserve, resize, push_back, assign, append, etc.)
///   without passing allocator on every call.
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
