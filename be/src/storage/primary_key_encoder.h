// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

// Utility methods to encode composite primary key into single binary key, while
// preserving original sort order.
// Currently only bool, integral types(tinyint, smallint, int, bigint, largeint)
// and varchar is supported.
// The encoding method is borrowed from kudu, e.g.
// For a composite key (k1, k2, k3), each key column is encoded and then append
// to the output binary key. Encode method for each type:
// bool, tinyint: append single byte
// smallint, int, bigint, largeint: convert to bigendian and append
// varchar:
//   if this column is the last column: append directly
//   if not: convert each 0x00 inside the string to 0x00 0x01,
//           add a tailing 0x00 0x00, then append
class PrimaryKeyEncoder {
public:
    static bool is_supported(const vectorized::Field& f);

    static bool is_supported(const vectorized::Schema& schema, const std::vector<ColumnId>& key_idxes);

    // Return |OLAP_FIELD_TYPE_NONE| if no primary key contained in |schema|.
    static FieldType encoded_primary_key_type(const vectorized::Schema& schema, const std::vector<ColumnId>& key_idxes);

    // Return -1 if encoded key is not fixed size
    static size_t get_encoded_fixed_size(const vectorized::Schema& schema);

    // create suitable column to hold encoded key
    //   schema: schema of the table
    //   pcolumn: output column
    //   large_column: some usage may fill the column with more than uint32_max elements, set true to support this
    static Status create_column(const vectorized::Schema& schema, std::unique_ptr<vectorized::Column>* pcolumn,
                                bool large_column = false);

    // create suitable column to hold encoded key
    //   schema: schema of the table
    //   pcolumn: output column
    //   key_idxes: indexes of columns for encoding
    //   large_column: some usage may fill the column with more than uint32_max elements, set true to support this
    static Status create_column(const vectorized::Schema& schema, std::unique_ptr<vectorized::Column>* pcolumn,
                                const std::vector<ColumnId>& key_idxes, bool large_column = false);

    static void encode(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset, size_t len,
                       vectorized::Column* dest);

    static void encode_sort_key(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                size_t len, vectorized::Column* dest);

    static void encode_selective(const vectorized::Schema& schema, const vectorized::Chunk& chunk,
                                 const uint32_t* indexes, size_t len, vectorized::Column* dest);

    static bool encode_exceed_limit(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                    size_t len, size_t limit_size);

    static Status decode(const vectorized::Schema& schema, const vectorized::Column& keys, size_t offset, size_t len,
                         vectorized::Chunk* dest);
};

} // namespace starrocks
