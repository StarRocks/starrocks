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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "base/string/slice.h"
#include "common/status.h"
#include "common/statusor.h"

namespace starrocks::sstable {

class Iterator;

// Name of the meta block that holds the key dictionary of a table.
inline constexpr std::string_view kKeyDictMetaBlockName = "starrocks.key_dict";

// Layout of one column of a composite primary key, as PrimaryKeyEncoder lays it out in the key bytes:
// a fixed-width big-endian integer, or a VARCHAR. A VARCHAR that is not the last column is escaped
// (0x00 -> 0x00 0x01) and ends with 0x00 0x00; the last column is the raw bytes up to the end of the key.
struct KeyColumnLayout {
    // Width in bytes of a fixed-width column; 0 for a VARCHAR column.
    uint32_t fixed_width = 0;
};
using KeyLayout = std::vector<KeyColumnLayout>;

// A per-sstable dictionary for the low-cardinality columns of composite primary keys.
//
// In segment files a low-cardinality key column is dictionary encoded and costs almost nothing, but in the
// primary key index sstables the whole encoded key is stored, so a column like `type` in PRIMARY KEY(id, type)
// is repeated in every entry: prefix compression cannot share it because it follows a high-cardinality
// column. A KeyDict replaces the bytes of such a column with a 1 or 2 byte code.
//
// The encoding preserves the bytewise order of the keys, so an encoded sstable is still sorted, and keys from
// different sstables, which may carry different dictionaries, can be compared once decoded. A column with n
// dictionary values uses the codes 0..2n: code 2i+1 stands for values[i]; code 2i is an escape, followed by
// the column's raw bytes, for a value that falls between values[i-1] and values[i]. Thanks to the escapes
// every key can be encoded, including keys whose values are not in the dictionary, and keys cut short in the
// middle of a column (seek targets such as index separators or range bounds); for those the encoding is the
// smallest stored key that is not less than the target.
class KeyDict {
public:
    struct ColumnDict {
        // Index of the column in the layout.
        uint32_t column = 0;
        // Distinct encoded byte segments of the column, sorted bytewise.
        std::vector<std::string> values;
    };

    // Largest dictionary a column can have: 2n + 1 codes must fit in 2 bytes.
    static constexpr uint32_t kMaxCardinality = 32767;

    // |columns| must be ordered by column index, and each one's values sorted, distinct and non-empty.
    static StatusOr<std::shared_ptr<const KeyDict>> create(KeyLayout layout, std::vector<ColumnDict> columns);
    static StatusOr<std::shared_ptr<const KeyDict>> deserialize(const Slice& data);

    void serialize(std::string* dst) const;

    // Encodes |key| into |dst| (replacing its contents). Never fails, see the class comment.
    void encode(const Slice& key, std::string* dst) const;
    // Decodes a stored key into |dst| (replacing its contents).
    Status decode(const Slice& stored, std::string* dst) const;

    const KeyLayout& layout() const { return _layout; }
    const std::vector<ColumnDict>& columns() const { return _columns; }
    size_t memory_usage() const { return _memory_usage; }

    static uint32_t code_width(size_t cardinality) { return cardinality <= 127 ? 1 : 2; }

private:
    KeyDict() = default;
    Status _init();

    KeyLayout _layout;
    std::vector<ColumnDict> _columns;
    // Position in _columns of each layout column's dictionary, or -1.
    std::vector<int32_t> _dict_of_column;
    // Number of leading layout columns that have to be parsed: up to and including the last dictionary column.
    size_t _parsed_columns = 0;
    size_t _memory_usage = 0;
};

// How a TableBuilder dictionary encodes the keys it is given. See TableBuilder::Add.
struct KeyDictOptions {
    // Default number of leading entries a builder looks at to choose a dictionary.
    static constexpr size_t kDefaultSampleEntries = 16384;

    KeyLayout layout;
    // A column with more distinct values than this is not dictionary encoded. At most KeyDict::kMaxCardinality.
    uint32_t max_cardinality = 256;
    // Use this dictionary as is. When null, the builder buffers the first |sample_entries| entries and chooses
    // the dictionary from them and |seeds|; a table with fewer entries gets an exact dictionary.
    std::shared_ptr<const KeyDict> dict;
    size_t sample_entries = kDefaultSampleEntries;
    // Dictionaries of data the keys are known to come from, e.g. the input sstables of a compaction.
    std::vector<std::shared_ptr<const KeyDict>> seeds;
};

// Chooses the dictionary for a set of keys added in ascending order.
//
// The first column is never encoded: prefix compression already stores it once per restart interval.
// Neither is a fixed-width column of one byte, which a code would not shrink. Any other column gets a
// dictionary if it has at most |max_cardinality| distinct values and the codes save at least one byte per key.
// When the keys are only a sample (the leading entries of a sorted stream), a column whose values never went
// down is skipped unless a seed covers it: such a column is sorted within the sample, so the sample has seen
// only the start of its value range and most later keys would need escapes.
class KeyDictCollector {
public:
    KeyDictCollector(KeyLayout layout, uint32_t max_cardinality);

    void add_seed(const KeyDict& dict);
    void add(const Slice& key);
    // True once no column can get a dictionary any more, so the remaining keys need not be added.
    bool exhausted() const;
    // |complete|: all the keys of the table went through add(), not only a sample.
    // Returns null when no column qualifies.
    StatusOr<std::shared_ptr<const KeyDict>> finish(bool complete);

private:
    struct ColumnState {
        bool candidate = false;
        bool overflow = false;
        bool seeded = false;
        bool decreased = false;
        uint64_t bytes = 0;
        uint64_t count = 0;
        std::string last;
        std::set<std::string, std::less<>> values;
    };

    void _insert(ColumnState* state, const Slice& value);

    KeyLayout _layout;
    uint32_t _max_cardinality;
    std::vector<ColumnState> _states;
    size_t _parsed_columns = 0;
};

// Wraps an iterator over dictionary encoded keys: key() returns decoded keys and Seek() takes decoded targets.
// Takes ownership of |iter|.
Iterator* NewKeyDictIterator(Iterator* iter, std::shared_ptr<const KeyDict> dict);

} // namespace starrocks::sstable
