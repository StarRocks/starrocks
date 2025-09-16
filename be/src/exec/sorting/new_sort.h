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

#include <sys/mman.h>

#include <span>
#include <vector>

#include "column/column.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "util/slice.h"

namespace starrocks {

// An object of this class can only be created when a proxy reference is fully dereferenced. This
// can happen in sort algorithms that store the pivot on the stack.
// This class should only be used in assignments to proxy references.
class InlinedDataStackVar {
public:
    InlinedDataStackVar(std::span<const char> data) : _data(std::begin(data), std::end(data)) {}

    std::span<char> data() { return std::span<char>{_data}; }
    std::span<const char> data() const { return std::span<const char>{_data}; }

private:
    // TODO Store small data inline (on the stack)
    std::vector<char> _data;
};

class InlinedDataProxy {
public:
    InlinedDataProxy(std::span<char> data) : data{data} {}
    InlinedDataProxy(const InlinedDataProxy& other) = default;
    InlinedDataProxy(InlinedDataProxy&& other) = default;
    InlinedDataProxy(InlinedDataStackVar& temp) : data{temp.data()} {}

    ~InlinedDataProxy() = default;

    InlinedDataProxy& operator=(InlinedDataProxy&& other) noexcept {
        DCHECK(data.size() == other.data.size());
        std::ranges::copy(other.data, data.data());
        return *this;
    }

    InlinedDataProxy& operator=(const InlinedDataProxy& other) noexcept {
        DCHECK(data.size() == other.data.size());
        std::ranges::copy(other.data, data.data());
        return *this;
    }

    InlinedDataProxy& operator=(InlinedDataStackVar&& other) noexcept {
        DCHECK(data.size() == other.data().size());
        std::ranges::copy(other.data(), data.data());
        return *this;
    }

    InlinedDataProxy& operator=(const InlinedDataStackVar& other) noexcept {
        DCHECK(data.size() == other.data().size());
        std::ranges::copy(other.data(), data.data());
        return *this;
    }

    // creates a copy of the stored data;
    operator InlinedDataStackVar() { return InlinedDataStackVar(data); }

    friend void swap(InlinedDataProxy lhs, InlinedDataProxy rhs) {
        DCHECK(lhs.data.size() == rhs.data.size());
        std::swap_ranges(std::begin(lhs.data), std::end(lhs.data), std::begin(rhs.data));
    }

    // The named function are not strictly necessary, but nicely match with the use cases
    template <class T>
    const T& as_fixed_size(size_t offset) const {
        return as_type<T>(offset);
    }

    const bool& as_bool(size_t offset) const { return as_type<bool>(offset); }

    const Slice& as_slice(size_t offset) const { return as_type<Slice>(offset); }

private:
    template <class T>
    const T& as_type(size_t offset) const {
        DCHECK(offset + sizeof(T) <= data.size());
        DCHECK( (reinterpret_cast<uintptr_t>(data.data()) + offset) % alignof(T) == 0);
        return *reinterpret_cast<T*>(data.data() + offset);
    }

    std::span<char> data;
};

class ConstInlinedDataProxy {
public:
    ConstInlinedDataProxy(std::span<const char> data) : data{data} {}

    ~ConstInlinedDataProxy() = default;

    operator InlinedDataStackVar() { return InlinedDataStackVar(data); }
    // The named function are not strictly necessary, but nicely match with the use cases
    template <class T>
    const T& as_fixed_size(size_t offset) const {
        return as_type<T>(offset);
    }

    const bool& as_bool(size_t offset) const { return as_type<bool>(offset); }

    const Slice& as_slice(size_t offset) const { return as_type<Slice>(offset); }

private:
    template <class T>
    const T& as_type(size_t offset) const {
        DCHECK(offset + sizeof(T) <= data.size());
        DCHECK(offset % alignof(T) == 0);
        return *reinterpret_cast<const T*>(data.data() + offset);
    }

    std::span<const char> data;
};

template <class T>
class InlinedIteratorBase {
public:
    T& operator-=(ptrdiff_t x) { return *static_cast<T*>(this) += -x; }
    T& operator++() { return *static_cast<T*>(this) += 1; }
    T operator++(int) {
        T cpy{*static_cast<T*>(this)};
        operator++();
        return cpy;
    }
    T& operator--() { return *static_cast<T*>(this) += -1; }
    T operator--(int) {
        T cpy{*static_cast<T*>(this)};
        operator--();
        return cpy;
    }

    T operator+(ptrdiff_t diff) const {
        T cpy{*static_cast<const T*>(this)};
        cpy += diff;
        return cpy;
    }

    T operator-(ptrdiff_t diff) const {
        T cpy{*static_cast<const T*>(this)};
        cpy -= diff;
        return cpy;
    }
};

class InlinedDataIterator : public InlinedIteratorBase<InlinedDataIterator> {
public:
    using difference_type = ptrdiff_t;
    using value_type = InlinedDataStackVar;
    using reference = InlinedDataProxy;
    using iterator_category = std::random_access_iterator_tag;

    InlinedDataIterator() : _size{0}, _curr_pos{nullptr} {}
    InlinedDataIterator(size_t size, char* ptr) : _size{static_cast<ptrdiff_t>(size)}, _curr_pos{ptr} {}

    InlinedDataProxy operator*() const {
        return InlinedDataProxy(std::span<char>(_curr_pos, _size));
    }

    InlinedDataIterator& operator+=(ptrdiff_t x) {
        _curr_pos += _size * x;
        return *this;
    }

    ptrdiff_t operator-(InlinedDataIterator it) const {
        DCHECK(_size == it._size);
        return (_curr_pos - it._curr_pos) / _size;
    }
    using InlinedIteratorBase<InlinedDataIterator>::operator-;

    InlinedDataProxy operator[](ptrdiff_t offset) const { return *(*this + offset); }

    std::strong_ordering operator<=>(InlinedDataIterator other) const {
        DCHECK(_size == other._size);
        if (other._curr_pos == _curr_pos) {
            return std::strong_ordering::equal;
        }
        if (std::less<>{}(_curr_pos, other._curr_pos)) {
            return std::strong_ordering::less;
        }
        return std::strong_ordering::greater;
    }

    bool operator==(InlinedDataIterator other) const {
        DCHECK(_size == other._size);
        return _curr_pos == other._curr_pos;
        std::vector<bool> bla;
    }

private:
    ptrdiff_t _size;
    char* _curr_pos;
};

inline InlinedDataIterator operator+(ptrdiff_t n, const InlinedDataIterator& bla) {
    return bla + n;
}

static_assert(std::random_access_iterator<InlinedDataIterator>);

using InlinedDataView = std::ranges::subrange<InlinedDataIterator>;

// Behaves like a reference proxy.
class ConstInlinedDataIterator : public InlinedIteratorBase<ConstInlinedDataIterator> {
    // Defenitions needed for an autogeneration of the iterator traits
public:
    using difference_type = ptrdiff_t;
    using value_type = InlinedDataStackVar;
    using reference = ConstInlinedDataProxy;
    using iterator_category = std::random_access_iterator_tag;

    ConstInlinedDataIterator(size_t element_size, const char* ptr)
            : size{static_cast<ptrdiff_t>(element_size)}, curr_pos{ptr} {}
    ConstInlinedDataIterator() : size{0}, curr_pos{nullptr} {}

    ConstInlinedDataProxy operator*() const { return ConstInlinedDataProxy(std::span<const char>(curr_pos, size)); }

    ConstInlinedDataIterator& operator+=(ptrdiff_t x) {
        curr_pos += size * x;
        return *this;
    }
    ptrdiff_t operator-(ConstInlinedDataIterator it) const { return (curr_pos - it.curr_pos) / size; }
    using InlinedIteratorBase<ConstInlinedDataIterator>::operator-;

    ConstInlinedDataProxy operator[](ptrdiff_t offset) const { return *(*this + offset); }

    std::strong_ordering operator<=>(ConstInlinedDataIterator other) const {
        if (other.curr_pos == curr_pos) {
            return std::strong_ordering::equal;
        }
        if (std::less<>{}(curr_pos, other.curr_pos)) {
            return std::strong_ordering::less;
        }
        return std::strong_ordering::greater;
    }

    bool operator==(ConstInlinedDataIterator other) const {
        DCHECK(size == other.size);
        return curr_pos == other.curr_pos;
    }

private:
    ptrdiff_t size;
    const char* curr_pos;
};

inline ConstInlinedDataIterator operator+(ptrdiff_t n, const ConstInlinedDataIterator& bla) {
    return bla + n;
}

static_assert(std::random_access_iterator<ConstInlinedDataIterator>);

struct InlinedData {
public:
    using index_type = uint64_t;

    InlinedData(std::vector<size_t>&& sizes, std::vector<size_t>&& alignments, size_t size)
            : offsets{init_offsets(std::move(sizes), std::move(alignments))}, data(size * offsets.back()) {}

    ConstInlinedDataIterator begin() const { return ConstInlinedDataIterator(entry_size(), data.data()); }

    ConstInlinedDataIterator end() const { return ConstInlinedDataIterator(entry_size(), data.data() + data.size()); }

    InlinedDataIterator begin() { return InlinedDataIterator(entry_size(), data.data()); }

    InlinedDataIterator end() { return InlinedDataIterator(entry_size(), data.data() + data.size()); }

    size_t entry_size() const { return offsets.back(); }

    size_t size() const { return data.size() / entry_size(); }

    template <class T>
    void set(size_t row_idx, size_t inlined_col_idx, const T& val) {
        // We will just deallocate the char vector, so the inserted entries need to be trivially destructible
        static_assert(std::is_trivially_destructible_v<T>);
        const ptrdiff_t global_entry_position = row_idx * entry_size() + offsets.at(inlined_col_idx);
        DCHECK(global_entry_position % alignof(T) == 0);
        DCHECK_LE(offsets.at(inlined_col_idx) + sizeof(val), offsets.at(inlined_col_idx + 1));
        T& entry = reinterpret_cast<T&>(data[global_entry_position]);
        entry = val;
    }

    template <class T>
    auto get_value(size_t inlined_col_index) {
        auto offset = offsets.at(inlined_col_index);
        return [offset](InlinedDataProxy proxy) -> T { return proxy.as_fixed_size<T>(offset); };
    }

    auto get_str(size_t inlined_col_index) {
        auto offset = offsets.at(inlined_col_index);
        return [offset](InlinedDataProxy proxy) -> Slice { return proxy.as_slice(offset); };
    }

    auto get_idx() {
        auto offset = *(offsets.end() - 2);
        return [offset](InlinedDataProxy proxy) -> index_type { return proxy.as_fixed_size<index_type>(offset); };
    }

    template <class T>
    auto get_value(size_t inlined_col_index) const {
        auto offset = offsets.at(inlined_col_index);
        return [offset](ConstInlinedDataProxy proxy) -> T { return proxy.as_fixed_size<T>(offset); };
    }

    auto get_str(size_t inlined_col_index) const {
        auto offset = offsets.at(inlined_col_index);
        return [offset](ConstInlinedDataProxy proxy) -> Slice { return proxy.as_slice(offset); };
    }

    auto get_idx() const {
        auto offset = *(offsets.end() - 2);
        return [offset](ConstInlinedDataProxy proxy) -> index_type { return proxy.as_fixed_size<index_type>(offset); };
    }

private:
    size_t round_up_to_next_aligned_position(size_t curr_offset, size_t next_alignment) {
        return (curr_offset / next_alignment + (curr_offset % next_alignment != 0)) * next_alignment;
    }

    std::vector<size_t> init_offsets(std::vector<size_t>&& sizes, std::vector<size_t>&& alignments) {
        DCHECK_EQ(sizes.size(), alignments.size());

        // Add size info for the row index
        sizes.push_back(sizeof(index_type));
        alignments.push_back(alignof(index_type));


        std::vector<size_t> offsets{};
        offsets.reserve(sizes.size() + 2);
        offsets.push_back(0);
        for (size_t i = 0; i + 1 < sizes.size(); ++i) {
            const auto curr_size = sizes.at(i);
            const auto next_alignment = alignments.at(i + 1);
            const auto next_offset = round_up_to_next_aligned_position(offsets.back() + curr_size, next_alignment);
            offsets.push_back(next_offset);
        }

        // The start address of every object needs to be aligned to the maximum alignment that appears in the object
        const size_t max_alignment = *std::max_element(std::begin(alignments), std::end(alignments));
        const auto curr_size = sizes.back();
        const auto next_offset = round_up_to_next_aligned_position(curr_size + offsets.back(), max_alignment);
        // The total size of an entry
        offsets.push_back(next_offset);

        sizes.clear();
        alignments.clear();

        return offsets;
    }

    std::vector<size_t> offsets;
    std::vector<char> data;
};

inline std::vector<size_t> get_order_by_indexes(const Columns& all_columns, const Columns& order_by_columns) {
    std::vector<size_t> indexes{};
    indexes.reserve(order_by_columns.size());
    for (size_t i = 0; i < order_by_columns.size(); ++i) {
        auto it = std::find(std::begin(all_columns), std::end(all_columns), order_by_columns.at(i).get());
        DCHECK(it != std::end(all_columns));
        indexes.push_back(it - std::begin(all_columns));
    }
    return indexes;
}

Status inline_data(const Columns& cols, std::unique_ptr<InlinedData>& out);
Status materialize(Chunk& target_chunk, const ChunkPtr& src_chunk, const std::vector<size_t>& order_by_columns,
                   const InlinedData& inlined_data, SortRange range);
Status sort(const std::atomic<bool>& cancel, Columns& columns, InlinedData& inlined_data, const SortDescs& sort_descs);

} // namespace starrocks
