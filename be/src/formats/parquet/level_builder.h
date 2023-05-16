
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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <utility>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "formats/parquet/chunk_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::parquet {

/// Intermediate data passed between add_column_chunk functions.
/// Example(Int Array): ctx -> writeArrayColumnChunk -> ctx' -> writeIntColumnChunk
/// Immutable and thread-safe.
class LevelBuilderContext {
public:
    LevelBuilderContext(size_t num_levels, std::shared_ptr<std::vector<int16_t>> def_levels = nullptr,
                        int16_t max_def_level = 0, std::shared_ptr<std::vector<int16_t>> rep_levels = nullptr,
                        int16_t max_rep_level = 0)
            : _max_def_level(max_def_level),
              _max_rep_level(max_rep_level),
              _num_levels(num_levels),
              _def_levels(std::move(def_levels)),
              _rep_levels(std::move(rep_levels)) {
        DCHECK(_max_def_level == 0 || _def_levels != nullptr);
        DCHECK(_max_rep_level == 0 || _rep_levels != nullptr);
        DCHECK(_max_def_level == 0 || _num_levels == _def_levels->size());
        DCHECK(_max_rep_level == 0 || _num_levels == _rep_levels->size());
    }

public:
    const int16_t _max_def_level;
    const int16_t _max_rep_level;

    // count of def/rep levels.
    // May be larger than values count if there are any undefined values.
    const int64_t _num_levels;

    // def/rep_levels == nullptr iff max_def/rep_level == 0
    const std::shared_ptr<std::vector<int16_t>> _def_levels;
    const std::shared_ptr<std::vector<int16_t>> _rep_levels;
};

struct LevelBuilderResult {
    int64_t num_levels;
    int16_t* def_levels;
    int16_t* rep_levels;
    uint8_t* values;
    uint8_t* null_bitset;
};

// Convert columns of nested type into definition/repetition levels, which are required to write Parquet file.
class LevelBuilder {
public:
    // A callback function that will receive results from caller
    using CallbackFunction = std::function<void(const LevelBuilderResult&)>;

    LevelBuilder(TypeDescriptor type_desc, ::parquet::schema::NodePtr node);

    // Determine rep/def level information for the array.
    //
    // The callback will be invoked for each leaf Array that is a descendant of array.  Each leaf array is
    // processed in a depth first traversal-order.
    void write(const LevelBuilderContext& ctx, const ColumnPtr& col, const CallbackFunction& write_leaf_callback);

private:
    void _write_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                             const CallbackFunction& write_leaf_callback);

    void _write_boolean_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                     const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                     const CallbackFunction& write_leaf_callback);

    template <LogicalType lt, ::parquet::Type::type pt>
    void _write_int_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                 const CallbackFunction& write_leaf_callback);

    void _write_decimal128_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                        const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                        const CallbackFunction& write_leaf_callback);

    void _write_varchar_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                     const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                     const CallbackFunction& write_leaf_callback);

    void _write_date_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                  const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                  const CallbackFunction& write_leaf_callback);

    void _write_datetime_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                      const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                      const CallbackFunction& write_leaf_callback);

    void _write_array_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                   const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                   const CallbackFunction& write_leaf_callback);

    void _write_map_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                 const CallbackFunction& write_leaf_callback);

    void _write_struct_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                    const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                    const CallbackFunction& write_leaf_callback);

    std::shared_ptr<std::vector<uint8_t>> _make_null_bitset(size_t n, const uint8_t* nulls) const;

    std::shared_ptr<std::vector<int16_t>> _make_def_levels(const LevelBuilderContext& ctx,
                                                           const ::parquet::schema::NodePtr& node,
                                                           const uint8_t* nulls) const;

    std::shared_ptr<std::vector<int16_t>> _make_def_levels_branchless(const LevelBuilderContext& ctx,
                                                                      const ::parquet::schema::NodePtr& node,
                                                                      const uint8_t* nulls,
                                                                      const size_t col_size) const;

private:
    TypeDescriptor _type_desc;
    ::parquet::schema::NodePtr _root;
};

} // namespace starrocks::parquet
