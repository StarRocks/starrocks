// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/bytes.h"
#include "column/column.h"
#include "column/datum.h"
#include "util/slice.h"

namespace starrocks::vectorized {

class BinaryColumn final : public ColumnFactory<Column, BinaryColumn> {
    friend class ColumnFactory<Column, BinaryColumn>;

public:
    using ValueType = Slice;

    using Offset = uint32_t;
    using Offsets = Buffer<uint32_t>;

    using Bytes = starrocks::raw::RawVectorPad16<uint8_t>;

    using Container = Buffer<Slice>;

    // TODO(kks): when we create our own vector, we could let vector[-1] = 0,
    // and then we don't need explicitly emplace_back zero value
    BinaryColumn() { _offsets.emplace_back(0); }
    BinaryColumn(Bytes bytes, Offsets offsets) : _bytes(std::move(bytes)), _offsets(std::move(offsets)) {
        if (_offsets.empty()) {
            _offsets.emplace_back(0);
        }
    };

    // NOTE: do *NOT* copy |_slices|
    BinaryColumn(const BinaryColumn& rhs) : _bytes(rhs._bytes), _offsets(rhs._offsets) {}

    // NOTE: do *NOT* copy |_slices|
    BinaryColumn(BinaryColumn&& rhs) noexcept : _bytes(std::move(rhs._bytes)), _offsets(std::move(rhs._offsets)) {}

    BinaryColumn& operator=(const BinaryColumn& rhs) {
        BinaryColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    BinaryColumn& operator=(BinaryColumn&& rhs) noexcept {
        BinaryColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~BinaryColumn() override {
        if (!_offsets.empty()) {
            DCHECK_EQ(_bytes.size(), _offsets.back());
        } else {
            DCHECK_EQ(_bytes.size(), 0);
        }
    }

    bool low_cardinality() const override { return false; }
    bool is_binary() const override { return true; }

    const uint8_t* raw_data() const override {
        if (!_slices_cache) {
            _build_slices();
        }
        return reinterpret_cast<const uint8_t*>(_slices.data());
    }

    uint8_t* mutable_raw_data() override {
        if (!_slices_cache) {
            _build_slices();
        }
        return reinterpret_cast<uint8_t*>(_slices.data());
    }

    size_t size() const override { return _offsets.size() - 1; }

    size_t capacity() const override { return _offsets.capacity() - 1; }

    size_t type_size() const override { return sizeof(Slice); }

    size_t byte_size() const override { return _bytes.size() * sizeof(uint8_t) + _offsets.size() * sizeof(Offset); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        return (_offsets[from + size] - _offsets[from]) + size * sizeof(Offset);
    }

    size_t byte_size(size_t idx) const override { return _offsets[idx + 1] - _offsets[idx] + sizeof(uint32_t); }

    Slice get_slice(size_t idx) const {
        return Slice(_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    }

    // For n value, the offsets size is n + 1
    // For example, for string "I","love","you"
    // the _bytes array is "Iloveyou"
    // the _offsets array is [0,1,5,8]
    void reserve(size_t n) override {
        // hard to know how the best reserve size of |_bytes|, inaccurate reserve may
        // affect the performance.
        // _bytes.reserve(n * 4);
        _offsets.reserve(n + 1);
        _slices_cache = false;
    }

    // If you know the size of the Byte array in advance, you can call this method,
    // n means the number of strings, byte_size is the total length of the string
    void reserve(size_t n, size_t byte_size) {
        _offsets.reserve(n + 1);
        _bytes.reserve(byte_size);
        _slices_cache = false;
    }

    void resize(size_t n) override {
        _offsets.resize(n + 1, _offsets.back());
        _bytes.resize(_offsets.back());
        _slices_cache = false;
    }

    void assign(size_t n, size_t idx) override;

    void remove_first_n_values(size_t count) override;

    void append(const Slice& str) {
        _bytes.insert(_bytes.end(), str.data, str.data + str.size);
        _offsets.emplace_back(_bytes.size());
        _slices_cache = false;
    }

    void append_datum(const Datum& datum) override {
        append(datum.get_slice());
        _slices_cache = false;
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    void append_string(const std::string& str) {
        _bytes.insert(_bytes.end(), str.data(), str.data() + str.size());
        _offsets.emplace_back(_bytes.size());
        _slices_cache = false;
    }

    bool append_strings(const std::vector<Slice>& strs) override;

    bool append_strings_overflow(const std::vector<Slice>& strs, size_t max_length) override;

    bool append_continuous_strings(const std::vector<Slice>& strs) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override {
        _offsets.emplace_back(_bytes.size());
        _slices_cache = false;
    }

    void append_default(size_t count) override {
        _offsets.insert(_offsets.end(), count, _bytes.size());
        _slices_cache = false;
    }

    uint32_t max_one_element_serialize_size() const override;

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(std::vector<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override { return sizeof(uint32_t) + _offsets[idx + 1] - _offsets[idx]; }

    MutableColumnPtr clone_empty() const override { return create_mutable(); }

    ColumnPtr cut(size_t start, size_t length) const;
    size_t filter_range(const Column::Filter& filter, size_t start, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override { return "binary"; }

    Container& get_data() {
        if (!_slices_cache) {
            _build_slices();
        }
        return _slices;
    }
    const Container& get_data() const {
        if (!_slices_cache) {
            _build_slices();
        }
        return _slices;
    }

    Bytes& get_bytes() { return _bytes; }

    const Bytes& get_bytes() const { return _bytes; }

    Offsets& get_offset() { return _offsets; }
    const Offsets& get_offset() const { return _offsets; }

    Datum get(size_t n) const override { return Datum(get_slice(n)); }

    size_t container_memory_usage() const override {
        return _bytes.capacity() + _offsets.capacity() * sizeof(_offsets[0]) + _slices.capacity() * sizeof(_slices[0]);
    }

    size_t shrink_memory_usage() const override {
        return _bytes.size() * sizeof(uint8_t) + _offsets.size() * sizeof(_offsets[0]) +
               _slices.size() * sizeof(_slices[0]);
    }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<BinaryColumn&>(rhs);
        using std::swap;
        swap(_delete_state, r._delete_state);
        swap(_bytes, r._bytes);
        swap(_offsets, r._offsets);
        swap(_slices, r._slices);
        swap(_slices_cache, r._slices_cache);
    }

    void reset_column() override {
        Column::reset_column();
        // TODO(zhuming): shrink size if needed.
        _bytes.clear();
        _offsets.resize(1, 0);
        _slices.clear();
        _slices_cache = false;
    }

    void invalidate_slice_cache() { _slices_cache = false; }

    std::string debug_item(uint32_t idx) const override;

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "[";
        size_t size = this->size();
        for (int i = 0; i < size - 1; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (size > 0) {
            ss << debug_item(size - 1);
        }
        ss << "]";
        return ss.str();
    }

    bool reach_capacity_limit() const override {
        return _bytes.size() >= Column::MAX_CAPACITY_LIMIT || _offsets.size() >= Column::MAX_CAPACITY_LIMIT ||
               _slices.size() >= Column::MAX_CAPACITY_LIMIT;
    }

private:
    void _build_slices() const;

    Bytes _bytes;
    Offsets _offsets;

    mutable Container _slices;
    mutable bool _slices_cache = false;
};

using Offsets = BinaryColumn::Offsets;
} // namespace starrocks::vectorized
