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

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "column/column_visitor.h"
#include "column/column_visitor_mutable.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gutil/casts.h"
#include "storage/delete_condition.h" // for DelCondSatisfied

namespace starrocks {

class MemPool;
class MysqlRowBuffer;
class Slice;
struct TypeDescriptor;

// Forward declaration
class Datum;

class Column {
public:
    // we use append fixed size to achieve faster memory copy.
    // We copy 350M rows, which total length is 2GB, max length is 15.
    // When size is 0, it means copy the string's actual size.
    // When size is other values, it means we copy the fixed length, which means we will copy
    // more bytes for each field.
    // Following is my test result.
    // size | time
    // 0    |   8s036ms
    // 16   |   3s485ms
    // 32   |   4s630ms
    // 64   |   5s127ms
    // 128  |   5s899ms
    // 256  |   8s210ms
    // From the result, we can see when fixed length is 128, we can get speed up for column read.
    enum { APPEND_OVERFLOW_MAX_SIZE = 128 };

    static const uint64_t MAX_CAPACITY_LIMIT = static_cast<uint64_t>(UINT32_MAX) + 1;
    static const uint64_t MAX_LARGE_CAPACITY_LIMIT = UINT64_MAX;

    static const int EQUALS_FALSE = 0;
    static const int EQUALS_NULL = -1;
    static const int EQUALS_TRUE = 1;

    // mutable operations cannot be applied to shared data when concurrent
    using Ptr = std::shared_ptr<Column>;
    // mutable means you could modify the data safely
    using MutablePtr = std::unique_ptr<Column>;

    virtual ~Column() = default;

    // If true means this is a null literal column
    virtual bool only_null() const { return false; }

    virtual bool is_nullable() const { return false; }

    virtual bool has_null() const { return false; }

    virtual bool is_null(size_t idx) const { return false; }

    virtual bool is_numeric() const { return false; }

    virtual bool is_constant() const { return false; }

    virtual bool is_binary() const { return false; }

    virtual bool is_large_binary() const { return false; }

    virtual bool is_decimal() const { return false; }

    virtual bool is_date() const { return false; }

    virtual bool is_timestamp() const { return false; }

    virtual bool is_object() const { return false; }

    virtual bool is_array() const { return false; }

    virtual bool is_map() const { return false; }

    virtual bool is_struct() const { return false; }

    virtual const uint8_t* raw_data() const = 0;

    virtual uint8_t* mutable_raw_data() = 0;

    virtual const uint8_t* continuous_data() const { return raw_data(); }

    // Return number of values in column.
    virtual size_t size() const = 0;

    virtual size_t capacity() const = 0;

    bool empty() const { return size() == 0; }

    virtual size_t type_size() const = 0;

    // Size of column data in memory (may be approximate). Zero, if could not be determined.
    virtual size_t byte_size() const = 0;
    virtual size_t byte_size(size_t from, size_t size) const = 0;

    // The byte size for serialize, for varchar, we need to add the len byte size
    virtual size_t byte_size(size_t idx) const = 0;

    virtual void reserve(size_t n) = 0;

    virtual void resize(size_t n) = 0;

    // If the column has already overflowed, upgrade to one larger Column type,
    // Return internal error if upgrade failed.
    // Return null, if the column is not overflow.
    // Return the new larger column, if upgrade success
    // Current, only support upgrade BinaryColumn to LargeBinaryColumn
    virtual StatusOr<ColumnPtr> upgrade_if_overflow() = 0;

    // Downgrade the column from large column to normal column.
    // Return internal error if downgrade failed.
    // Return null, if the column is already normal column, no need to downgrade.
    // Return the new normal column, if downgrade success
    // Current, only support downgrade LargeBinaryColumn to BinaryColumn
    virtual StatusOr<ColumnPtr> downgrade() = 0;

    // Check if the column contains large column.
    // Current, only used to check if it contains LargeBinaryColumn or BinaryColumn
    virtual bool has_large_column() const = 0;

    virtual void resize_uninitialized(size_t n) { resize(n); }

    // Assign specified idx element to the column container content,
    // and modifying column size accordingly.
    virtual void assign(size_t n, size_t idx) = 0;

    // Appends one value at the end of column (column's size is increased by 1).
    virtual void append_datum(const Datum& datum) = 0;

    virtual void remove_first_n_values(size_t count) = 0;

    // Append |count| elements from |src|, started from the offset |offset|, into |this| column.
    // It's undefined behaviour if |offset+count| greater than the size of |src|.
    // The type of |src| and |this| must be exactly matched.
    virtual void append(const Column& src, size_t offset, size_t count) = 0;

    virtual void append(const Column& src) { append(src, 0, src.size()); }

    // replicate a column to align with an array's offset, used for captured columns in lambda functions
    // for example: column(1,2)->replicate({0,2,5}) = column(1,1,2,2,2)
    // FixedLengthColumn, BinaryColumn and ConstColumn override this function for better performance.
    // TODO(fzh): optimize replicate() for ArrayColumn, ObjectColumn and others.
    virtual ColumnPtr replicate(const std::vector<uint32_t>& offsets) {
        auto dest = this->clone_empty();
        auto dest_size = offsets.size() - 1;
        DCHECK(this->size() >= dest_size) << "The size of the source column is less when duplicating it.";
        dest->reserve(offsets.back());
        for (int i = 0; i < dest_size; ++i) {
            dest->append_value_multiple_times(*this, i, offsets[i + 1] - offsets[i]);
        }
        return dest;
    }
    // Update elements to default value which hit by the filter
    virtual void fill_default(const Filter& filter) = 0;

    // This function will update data from src according to the input indexes. 'indexes' contains
    // the row index will be update
    // For example:
    //      input indexes: [0, 3]
    //      column data: [0, 1, 2, 3, 4]
    //      src_column data: [5, 6]
    // After call this function, column data will be set as [5, 1, 2, 6, 4]
    // The values in indexes is incremented
    virtual void update_rows(const Column& src, const uint32_t* indexes) = 0;

    // This function will append data from src according to the input indexes. 'indexes' contains
    // the row index of the src.
    // This function will get row index from indexes and append the data to this column.
    // This function will handle indexes start from input 'from' and will append 'size' times
    // For example:
    //      input indexes: [5, 4, 3, 2, 1]
    //      from: 2
    //      size: 2
    // This function will copy the [3, 2] row of src to this column.
    virtual void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) = 0;

    void append_selective(const Column& src, const Buffer<uint32_t>& indexes) {
        return append_selective(src, indexes.data(), 0, static_cast<uint32_t>(indexes.size()));
    }

    // This function will get row through 'from' index from src, and copy size elements to this column.
    // Currently only `ObjectColumn<BitmapValue>` support shallow copy
    virtual void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) = 0;

    // Append multiple `null` values into this column.
    // Return false if this is a non-nullable column, i.e, if `is_nullable` return false.
    virtual bool append_nulls(size_t count) = 0;

    // Append multiple strings into this column.
    // Return false if the column is not a binary column.
    [[nodiscard]] virtual bool append_strings(const Buffer<Slice>& strs) = 0;

    // Like append_strings. To achieve higher performance, this function will read 16 bytes out of
    // bounds. So the caller must make sure that no invalid address access exception occurs for
    // out-of-bounds reads
    [[nodiscard]] virtual bool append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) { return false; }

    // Like `append_strings` but the corresponding storage of each slice is adjacent to the
    // next one's, the implementation can take advantage of this feature, e.g, copy the whole
    // memory at once.
    [[nodiscard]] virtual bool append_continuous_strings(const Buffer<Slice>& strs) { return append_strings(strs); }

    [[nodiscard]] virtual bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
        return false;
    }

    // Copy |length| bytes from |buff| into this column and cast them as integers.
    // The count of copied integers depends on |length| and the size of column value:
    //  - `int8_t` column:  |length| integers will be copied.
    //  - `int16_t` column: |length| / 2 integers will be copied.
    //  - `int32_t` column: |length| / 4 integers will be copied.
    //  - ...
    // |buff| must NOT be nullptr.
    // Return
    //  - the count of copied integers on success.
    //  - -1 if this is not a numeric column.
    [[nodiscard]] virtual size_t append_numbers(const void* buff, size_t length) = 0;

    // Append |*value| |count| times, this is only used when load default value.
    virtual void append_value_multiple_times(const void* value, size_t count) = 0;

    // Append one default value into this column.
    // NOTE:
    //  - for `NullableColumn`, the default value is `null`.
    //  - for `BinaryColumn`, the default value is empty string.
    //  - for `FixedLengthColumn`, the default value is zero.
    //  - for `ConstColumn`, the default value is the const value itself.
    virtual void append_default() = 0;

    // Append multiple default values into this column.
    virtual void append_default(size_t count) = 0;

    // Sometimes(Hash group by multi columns),
    // we need one buffer to hold tmp serialize data,
    // So we need to know the max serialize_size for all column element
    // The bad thing is we couldn't get the string defined len from FE when query
    virtual uint32_t max_one_element_serialize_size() const {
        return 16; // For Non-string type, 16 is enough.
    }

    // serialize one data,The memory must allocate firstly from mempool
    virtual uint32_t serialize(size_t idx, uint8_t* pos) = 0;

    // serialize default value of column
    // The behavior is consistent with append_default
    virtual uint32_t serialize_default(uint8_t* pos) = 0;

    virtual void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                 uint32_t max_one_row_size) = 0;

    // A dedicated serialization method used by HashJoin to combine multiple columns into a wide-key
    // column, and it's only implemented by numeric columns right now.
    // This method serializes its elements one by one into the destination buffer starting at
    // (dst + byte_offset) with an interval between each element. It returns size of the data type
    // (which should be fixed size) of this column if this column supports this method, otherwise
    // it returns 0.
    virtual size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                               size_t count) {
        return 0;
    };

    // A dedicated serialization method used by NullableColumn to serialize data columns with null_masks.
    virtual void serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                                 uint32_t max_one_row_size, uint8_t* null_masks, bool has_null);

    // deserialize one data and append to this column
    virtual const uint8_t* deserialize_and_append(const uint8_t* pos) = 0;

    virtual void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) = 0;

    // One element serialize_size
    virtual uint32_t serialize_size(size_t idx) const = 0;

    // return new empty column with the same type
    virtual MutablePtr clone_empty() const = 0;

    virtual MutablePtr clone() const = 0;

    // clone column
    virtual Ptr clone_shared() const = 0;

    // REQUIRES: size of |filter| equals to the size of this column.
    // Removes elements that don't match the filter.
    inline size_t filter(const Filter& filter) {
        DCHECK_EQ(size(), filter.size());
        return filter_range(filter, 0, filter.size());
    }

    inline size_t filter(const Filter& filter, size_t count) { return filter_range(filter, 0, count); }

    // get rid of the case where the map/array is null but the map/array'elements are not empty.
    bool empty_null_in_complex_column(const Filter& null_data, const std::vector<uint32_t>& offsets);

    // FIXME: Many derived implementation assume |to| equals to size().
    virtual size_t filter_range(const Filter& filter, size_t from, size_t to) = 0;

    // Compares (*this)[left] and rhs[right]. Column rhs should have the same type.
    // Returns negative number, 0, or positive number (*this)[left] is less, equal, greater than
    // rhs[right] respectively.
    //
    // If one of element's value is NaN or NULLs, then:
    // - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
    // - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
    // For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
    //
    // For non Nullable and non floating point types, nan_direction_hint is ignored.
    virtual int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const = 0;

    // For some columns equals will be overwritten for more efficient
    // When safe equals, 0: false, 1: true
    // When unsafe equals, -1: NULL, 0: false, 1: true
    // return: EQUALS_FALSE, EQUALS_NULL, EQUALS_TRUE
    virtual int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const {
        return compare_at(left, right, rhs, -1) == 0;
    }

    // Compute fvn hash, mainly used by shuffle column data
    // Note: shuffle hash function should be different from Aggregate and Join Hash map hash function
    virtual void fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const = 0;

    // used by data loading compute tablet bucket
    virtual void crc32_hash(uint32_t* seed, uint32_t from, uint32_t to) const = 0;

    virtual void crc32_hash_at(uint32_t* seed, uint32_t idx) const { crc32_hash(seed - idx, idx, idx + 1); }

    virtual void fnv_hash_at(uint32_t* seed, uint32_t idx) const { fnv_hash(seed - idx, idx, idx + 1); }

    // For iceberg bucket join.
    // If there are multiple bucket columns <column, bucket_num> such as <c1, 50>, <c2, 60>, <c3, 10>.
    // we use the following formula as iceberg unique bucket id.
    // bucket_id = c1_bucket_id * 60 * 10 + c2_bucket_id * 10 + c3_bucket_id
    // TODO(stephen): Currently, we only support some simple types, and will fully support all types in the future.
    virtual void murmur_hash3_x86_32(uint32_t* hash, uint32_t from, uint32_t to, int32_t* bucket_nums,
                                     int32_t step) const {};

    virtual int64_t xor_checksum(uint32_t from, uint32_t to) const = 0;

    // Push one row to MysqlRowBuffer
    virtual void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const = 0;

    void set_delete_state(DelCondSatisfied delete_state) { _delete_state = delete_state; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    virtual std::string get_name() const = 0;

    // Return the value of the n-th element.
    virtual Datum get(size_t n) const = 0;

    // return false if this is a non-nullable column.
    // |idx| must less than the size of column.
    [[nodiscard]] virtual bool set_null(size_t idx __attribute__((unused))) { return false; }

    // Only used for debug one item in this column
    virtual std::string debug_item(size_t idx) const { return ""; }

    virtual std::string debug_string() const { return {}; }

    // used for automatic partition item in this column
    virtual std::string raw_item_value(size_t idx) const { return debug_item(idx); }

    // memory usage includes container memory usage and reference memory usage.
    // 1. container memory usage: container capacity * type size.
    // 2. reference memory usage: element data size that is not in the container,
    //    such as memory referenced by pointer.
    //   2.1 object column: element serialize data size.
    //   2.2 other columns: 0.
    virtual size_t memory_usage() const { return container_memory_usage() + reference_memory_usage(); }
    virtual size_t container_memory_usage() const = 0;
    virtual size_t reference_memory_usage() const { return reference_memory_usage(0, size()); }
    virtual size_t reference_memory_usage(size_t from, size_t size) const = 0;

    virtual void swap_column(Column& rhs) = 0;

    virtual void reset_column() { _delete_state = DEL_NOT_SATISFIED; }

    virtual bool capacity_limit_reached(std::string* msg = nullptr) const = 0;

    virtual Status accept(ColumnVisitor* visitor) const = 0;

    virtual Status accept_mutable(ColumnVisitorMutable* visitor) = 0;

    virtual void check_or_die() const = 0;

    // NOTE(alvin): make sure that field can not be ConstColumn, it will cause a lot of problems.
    // Because ConstColumn is not handled well in every Column's append functions.
    // To handle it, ConstColumns must be converted into normal columns.
    // But if complex types contains ConstColumns internally, current unpack functions can not handle it.
    // So to get a right answer, we need to make sure that there are no const columns in Complex Columns(Struct/Map)
    virtual Status unfold_const_children(const TypeDescriptor& type) { return Status::OK(); }
    // current only used by adaptive_nullable_column
    virtual void materialized_nullable() const {}

protected:
    static StatusOr<ColumnPtr> downgrade_helper_func(ColumnPtr* col);
    static StatusOr<ColumnPtr> upgrade_helper_func(ColumnPtr* col);

    DelCondSatisfied _delete_state = DEL_NOT_SATISFIED;
};

// AncestorBase is root class of inheritance hierarchy
// if Derived class is the direct subclass of the root, then AncestorBase is just the Base class
// if Derived class is the indirect subclass of the root, Base class is parent class, and
// AncestorBase must be the root class. because Derived class need some type information from
// AncestorBase to override the virtual method. e.g. clone and clone_shared method.
template <typename Base, typename Derived, typename AncestorBase = Base>
class ColumnFactory : public Base {
private:
    Derived* mutable_derived() { return down_cast<Derived*>(this); }
    const Derived* derived() const { return down_cast<const Derived*>(this); }

public:
    template <typename... Args>
    ColumnFactory(Args&&... args) : Base(std::forward<Args>(args)...) {}
    // mutable operations cannot be applied to shared data when concurrent
    using Ptr = std::shared_ptr<Derived>;
    // mutable means you could modify the data safely
    using MutablePtr = std::unique_ptr<Derived>;
    using AncestorBaseType = std::enable_if_t<std::is_base_of_v<AncestorBase, Base>, AncestorBase>;

    template <typename... Args>
    static Ptr create(Args&&... args) {
        return std::make_shared<Derived>(std::forward<Args>(args)...);
    }

    template <typename... Args>
    static MutablePtr create_mutable(Args&&... args) {
        return std::make_unique<Derived>(std::forward<Args>(args)...);
    }

    template <typename T>
    static Ptr create(std::initializer_list<T>&& arg) {
        return std::make_shared<Derived>(std::move(arg));
    }

    template <typename T>
    static MutablePtr create_mutable(std::initializer_list<T>&& arg) {
        return std::make_unique<Derived>(std::move(arg));
    }

    typename AncestorBaseType::MutablePtr clone() const override {
        return typename AncestorBase::MutablePtr(new Derived(*derived()));
    }

    typename AncestorBaseType::Ptr clone_shared() const override {
        return typename AncestorBase::Ptr(new Derived(*derived()));
    }

    Status accept(ColumnVisitor* visitor) const override { return visitor->visit(*static_cast<const Derived*>(this)); }

    Status accept_mutable(ColumnVisitorMutable* visitor) override {
        return visitor->visit(static_cast<Derived*>(this));
    }
};

} // namespace starrocks
