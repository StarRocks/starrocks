
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

namespace starrocks {
namespace parquet {

class LevelBuilderContext {
public:
    LevelBuilderContext(int16_t max_def_level, int16_t max_rep_level, size_t estimated_size = 0)
            : _max_def_level(max_def_level),
              _max_rep_level(max_rep_level),
              _idx2subcol(std::make_shared<std::vector<int>>()),
              _def_levels(std::make_shared<std::vector<int16_t>>()),
              _rep_levels(std::make_shared<std::vector<int16_t>>()) {
        _idx2subcol->reserve(estimated_size);
        _def_levels->reserve(estimated_size);
        _rep_levels->reserve(estimated_size);
    }

    void append(int idx, int16_t def_level, int16_t rep_level) {
        _idx2subcol->push_back(idx);
        _def_levels->push_back(def_level);
        _rep_levels->push_back(rep_level);
    }

    std::tuple<int, int16_t, int16_t> get(int i) const {
        DCHECK_LT(i, size());
        return std::make_tuple(_idx2subcol->at(i), _def_levels->at(i), _rep_levels->at(i));
    }

    int size() const { return _def_levels->size(); }

public:
    const int16_t _max_def_level;
    const int16_t _max_rep_level;
    constexpr static int kNULL = -1;

    std::shared_ptr<std::vector<int>> _idx2subcol;
    std::shared_ptr<std::vector<int16_t>> _def_levels;
    std::shared_ptr<std::vector<int16_t>> _rep_levels;
};

struct LevelBuilderResult {
    int64_t num_levels;
    int16_t* def_levels;
    int16_t* rep_levels;
    uint8_t* values;
    uint8_t* null_bitset;
};

class LevelBuilder {
public:
    using CallbackFunction = std::function<void(const LevelBuilderResult&)>;

    LevelBuilder(TypeDescriptor type_desc, ::parquet::schema::NodePtr node);

    void write(const LevelBuilderContext& ctx, const ColumnPtr& col, const CallbackFunction& write_leaf_callback);

private:
    Status _write_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                               const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                               const CallbackFunction& write_leaf_callback);

    Status _write_boolean_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                       const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                       const CallbackFunction& write_leaf_callback);

    template <LogicalType lt, ::parquet::Type::type pt>
    Status _write_int_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                   const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                   const CallbackFunction& write_leaf_callback);

    Status _write_decimal128_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                          const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                          const CallbackFunction& write_leaf_callback);

    Status _write_varchar_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                       const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                       const CallbackFunction& write_leaf_callback);

    Status _write_date_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                    const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                    const CallbackFunction& write_leaf_callback);

    Status _write_datetime_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                        const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                        const CallbackFunction& write_leaf_callback);

    Status _write_array_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                     const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                     const CallbackFunction& write_leaf_callback);

    Status _write_map_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                   const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                   const CallbackFunction& write_leaf_callback);

    Status _write_struct_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                      const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                      const CallbackFunction& write_leaf_callback);

    std::vector<uint8_t> _make_null_bitset(size_t n, const uint8_t* nulls) const;

    std::shared_ptr<std::vector<int16_t>> _make_def_levels(const LevelBuilderContext& ctx,
                                                           const ::parquet::schema::NodePtr& node,
                                                           const uint8_t* nulls) const;

private:
    TypeDescriptor _type_desc;
    ::parquet::schema::NodePtr _root;
};

} // namespace parquet
} // namespace starrocks
