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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "column/column.h"
#include "types/datum.h"

namespace starrocks {
namespace {

class TestIntColumn final : public Column {
public:
    explicit TestIntColumn(bool is_array = false) : _is_array(is_array) {}

    bool is_array() const override { return _is_array; }

    const uint8_t* raw_data() const override { return reinterpret_cast<const uint8_t*>(_data.data()); }
    uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data.data()); }

    size_t size() const override { return _data.size(); }
    size_t capacity() const override { return _data.capacity(); }
    size_t type_size() const override { return sizeof(int32_t); }

    size_t byte_size() const override { return _data.size() * sizeof(int32_t); }
    size_t byte_size(size_t from, size_t sz) const override {
        size_t clamped_from = std::min(from, _data.size());
        size_t clamped_size = std::min(sz, _data.size() - clamped_from);
        return clamped_size * sizeof(int32_t);
    }
    size_t byte_size(size_t idx) const override { return idx < _data.size() ? sizeof(int32_t) : 0; }

    void reserve(size_t n) override { _data.reserve(n); }
    void resize(size_t n) override { _data.resize(n); }

    StatusOr<MutablePtr> upgrade_if_overflow() override { return nullptr; }
    StatusOr<MutablePtr> downgrade() override { return nullptr; }
    bool has_large_column() const override { return false; }

    void assign(size_t n, size_t idx) override { _data.assign(n, _data[idx]); }

    void append_datum(const Datum& datum) override { _data.emplace_back(datum.is_null() ? 0 : datum.get_int32()); }

    void remove_first_n_values(size_t count) override {
        size_t n = std::min(count, _data.size());
        _data.erase(_data.begin(), _data.begin() + n);
    }

    void append(const Column& src, size_t offset, size_t count) override {
        const auto* rhs = dynamic_cast<const TestIntColumn*>(&src);
        DCHECK(rhs != nullptr);
        DCHECK_LE(offset + count, rhs->_data.size());
        _data.insert(_data.end(), rhs->_data.begin() + offset, rhs->_data.begin() + offset + count);
    }

    void fill_default(const Filter& filter) override {
        DCHECK_EQ(filter.size(), _data.size());
        for (size_t i = 0; i < filter.size(); ++i) {
            if (filter[i] != 0) {
                _data[i] = 0;
            }
        }
    }

    void update_rows(const Column& src, const uint32_t* indexes) override {
        const auto* rhs = dynamic_cast<const TestIntColumn*>(&src);
        DCHECK(rhs != nullptr);
        for (size_t i = 0; i < rhs->_data.size(); ++i) {
            DCHECK_LT(indexes[i], _data.size());
            _data[indexes[i]] = rhs->_data[i];
        }
    }

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t sz) override {
        const auto* rhs = dynamic_cast<const TestIntColumn*>(&src);
        DCHECK(rhs != nullptr);
        for (uint32_t i = 0; i < sz; ++i) {
            uint32_t idx = indexes[from + i];
            DCHECK_LT(idx, rhs->_data.size());
            _data.emplace_back(rhs->_data[idx]);
        }
    }

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) override {
        const auto* rhs = dynamic_cast<const TestIntColumn*>(&src);
        DCHECK(rhs != nullptr);
        DCHECK_LT(index, rhs->_data.size());
        _data.insert(_data.end(), count, rhs->_data[index]);
    }

    bool append_nulls(size_t count) override {
        _data.insert(_data.end(), count, 0);
        return true;
    }

    size_t append_numbers(const void* buff, size_t length) override {
        DCHECK(buff != nullptr);
        size_t n = length / sizeof(int32_t);
        const auto* p = reinterpret_cast<const int32_t*>(buff);
        _data.insert(_data.end(), p, p + n);
        return n;
    }

    void append_value_multiple_times(const void* value, size_t count) override {
        const int32_t v = *reinterpret_cast<const int32_t*>(value);
        _data.insert(_data.end(), count, v);
    }

    void append_default() override { _data.emplace_back(0); }
    void append_default(size_t count) override { _data.insert(_data.end(), count, 0); }

    uint32_t max_one_element_serialize_size() const override { return sizeof(int32_t); }

    uint32_t serialize(size_t idx, uint8_t* pos) const override {
        memcpy(pos, &_data[idx], sizeof(int32_t));
        return sizeof(int32_t);
    }

    uint32_t serialize_default(uint8_t* pos) const override {
        static constexpr int32_t kDefault = 0;
        memcpy(pos, &kDefault, sizeof(int32_t));
        return sizeof(int32_t);
    }

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
        }
    }

    void deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size, Buffer<uint8_t>& is_nulls,
                                               bool& has_null) override {
        is_nulls.reserve(is_nulls.size() + chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            bool null = false;
            memcpy(&null, srcs[i].data, sizeof(bool));
            srcs[i].data += sizeof(bool);
            is_nulls.emplace_back(null);
            if (null) {
                has_null = true;
                append_default();
            } else {
                srcs[i].data = const_cast<char*>(reinterpret_cast<const char*>(
                        deserialize_and_append(reinterpret_cast<const uint8_t*>(srcs[i].data))));
            }
        }
    }

    const uint8_t* deserialize_and_append(const uint8_t* pos) override {
        int32_t value = 0;
        memcpy(&value, pos, sizeof(int32_t));
        _data.emplace_back(value);
        return pos + sizeof(int32_t);
    }

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override {
        for (size_t i = 0; i < chunk_size; ++i) {
            srcs[i].data = const_cast<char*>(reinterpret_cast<const char*>(
                    deserialize_and_append(reinterpret_cast<const uint8_t*>(srcs[i].data))));
        }
    }

    uint32_t serialize_size(size_t idx) const override { return idx < _data.size() ? sizeof(int32_t) : 0; }

    MutablePtr clone_empty() const override { return nullptr; }
    MutablePtr clone() const override { return nullptr; }

    size_t filter_range(const Filter& filter, size_t from, size_t to) override {
        std::vector<int32_t> kept;
        kept.reserve(_data.size());
        for (size_t i = 0; i < from; ++i) {
            kept.emplace_back(_data[i]);
        }
        for (size_t i = from; i < to; ++i) {
            if (filter[i] != 0) {
                kept.emplace_back(_data[i]);
            }
        }
        for (size_t i = to; i < _data.size(); ++i) {
            kept.emplace_back(_data[i]);
        }
        _data.swap(kept);
        return _data.size();
    }

    int compare_at(size_t left, size_t right, const Column& rhs, int) const override {
        const auto* r = dynamic_cast<const TestIntColumn*>(&rhs);
        DCHECK(r != nullptr);
        if (_data[left] == r->_data[right]) {
            return 0;
        }
        return _data[left] < r->_data[right] ? -1 : 1;
    }

    int64_t xor_checksum(uint32_t from, uint32_t to) const override {
        int64_t checksum = 0;
        for (uint32_t i = from; i < to && i < _data.size(); ++i) {
            checksum ^= _data[i];
        }
        return checksum;
    }

    void put_mysql_row_buffer(MysqlRowBuffer*, size_t, bool) const override {}

    std::string get_name() const override { return "TestIntColumn"; }

    Datum get(size_t n) const override { return Datum(_data[n]); }

    size_t container_memory_usage() const override { return _data.capacity() * sizeof(int32_t); }
    size_t reference_memory_usage(size_t, size_t) const override { return 0; }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<TestIntColumn&>(rhs);
        _data.swap(r._data);
        std::swap(_is_array, r._is_array);
    }

    Status capacity_limit_reached() const override { return Status::OK(); }
    Status accept(ColumnVisitor*) const override { return Status::NotSupported("not implemented in test"); }
    Status accept_mutable(ColumnVisitorMutable*) override { return Status::NotSupported("not implemented in test"); }
    void check_or_die() const override {}

    void append_value(int32_t v) { _data.emplace_back(v); }

private:
    std::vector<int32_t> _data;
    bool _is_array = false;
};

TEST(ColumnBaseTest, SerializeBatchWithNullMasksNoNull) {
    TestIntColumn col;
    col.append_value(10);
    col.append_value(20);

    Buffer<uint32_t> slice_sizes(2, 0);
    const uint32_t max_one_row_size = sizeof(bool) + sizeof(int32_t);
    std::vector<uint8_t> serialized(2 * max_one_row_size, 0);
    col.serialize_batch_with_null_masks(serialized.data(), slice_sizes, 2, max_one_row_size, nullptr, false);

    EXPECT_EQ(max_one_row_size, slice_sizes[0]);
    EXPECT_EQ(max_one_row_size, slice_sizes[1]);

    bool row0_null = true;
    bool row1_null = true;
    int32_t row0_value = 0;
    int32_t row1_value = 0;
    memcpy(&row0_null, serialized.data(), sizeof(bool));
    memcpy(&row1_null, serialized.data() + max_one_row_size, sizeof(bool));
    memcpy(&row0_value, serialized.data() + sizeof(bool), sizeof(int32_t));
    memcpy(&row1_value, serialized.data() + max_one_row_size + sizeof(bool), sizeof(int32_t));

    EXPECT_FALSE(row0_null);
    EXPECT_FALSE(row1_null);
    EXPECT_EQ(10, row0_value);
    EXPECT_EQ(20, row1_value);
}

TEST(ColumnBaseTest, SerializeBatchWithNullMasksHasNull) {
    TestIntColumn col;
    col.append_value(7);
    col.append_value(9);

    Buffer<uint32_t> slice_sizes(2, 0);
    const uint32_t max_one_row_size = sizeof(bool) + sizeof(int32_t);
    std::vector<uint8_t> serialized(2 * max_one_row_size, 0);
    std::vector<uint8_t> null_masks{0, 1};
    col.serialize_batch_with_null_masks(serialized.data(), slice_sizes, 2, max_one_row_size, null_masks.data(), true);

    EXPECT_EQ(max_one_row_size, slice_sizes[0]);
    EXPECT_EQ(sizeof(bool), slice_sizes[1]);
}

TEST(ColumnBaseTest, SerializeBatchAtIntervalWithNullMasks) {
    TestIntColumn col;
    col.append_value(77);
    col.append_value(88);

    std::vector<uint8_t> null_masks{0, 1};
    std::vector<uint8_t> buffer(32, 0xff);
    const size_t byte_offset = 2;
    const size_t byte_interval = 16;

    size_t type_size = col.serialize_batch_at_interval_with_null_masks(buffer.data(), byte_offset, byte_interval,
                                                                       sizeof(int32_t), 0, 2, null_masks.data());
    EXPECT_EQ(sizeof(int32_t), type_size);

    int32_t first_value = 0;
    int32_t second_value = 1;
    memcpy(&first_value, buffer.data() + byte_offset + 1, sizeof(int32_t));
    memcpy(&second_value, buffer.data() + byte_interval + byte_offset + 1, sizeof(int32_t));
    EXPECT_EQ(77, first_value);
    EXPECT_EQ(0, second_value);
}

TEST(ColumnBaseTest, EmptyNullInComplexColumnRejectsScalarColumn) {
    TestIntColumn col;
    col.append_value(1);

    Buffer<uint8_t> null_data{0};
    Buffer<uint32_t> offsets{0, 1};
    EXPECT_THROW(col.empty_null_in_complex_column(null_data, offsets), std::runtime_error);
}

TEST(ColumnBaseTest, AppendSelectiveToFailsOnNonViewColumn) {
    TestIntColumn src;
    src.append_value(1);
    TestIntColumn dst;
    uint32_t indexes[] = {0};

    EXPECT_DEATH(src.append_selective_to(dst, indexes, 0, 1),
                 "append_selective_to is only supported by view-like columns");
}

} // namespace
} // namespace starrocks
