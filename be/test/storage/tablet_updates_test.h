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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/kv_store.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/path_util.h"

namespace starrocks {

enum PartialUpdateCloneCase {
    CASE1, // rowset status is committed in meta, rowset file is partial rowset
    CASE2, // rowset status is committed in meta, rowset file is partial rowset, but rowset is apply success after link file
    CASE3, // rowset status is committed in meta, rowset file is full rowset
    CASE4  // rowset status is applied in meta, rowset file is full rowset
};

template <class T>
static void append_datum_func(ColumnPtr& col, T val) {
    if (val == -1) {
        col->append_nulls(1);
    } else {
        col->append_datum(Datum(val));
    }
}

class TabletUpdatesTest : public testing::Test {
public:
    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr, bool empty = false, bool has_merge_condition = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        if (has_merge_condition) {
            writer_context.merge_condition = "v2";
        }
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        if (empty) {
            return *writer->build();
        }
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            if (schema.num_key_fields() == 1) {
                cols[0]->append_datum(Datum(key));
            } else {
                cols[0]->append_datum(Datum(key));
                string v = fmt::to_string(key * 234234342345);
                cols[1]->append_datum(Datum(Slice(v)));
                cols[2]->append_datum(Datum((int32_t)key));
            }
            int vcol_start = schema.num_key_fields();
            cols[vcol_start]->append_datum(Datum((int16_t)(key % 100 + 1)));
            if (cols[vcol_start + 1]->is_binary()) {
                string v = fmt::to_string(key % 1000 + 2);
                cols[vcol_start + 1]->append_datum(Datum(Slice(v)));
            } else {
                cols[vcol_start + 1]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }
        }
        if (one_delete == nullptr && !keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else if (one_delete == nullptr) {
            CHECK_OK(writer->flush());
        } else if (one_delete != nullptr) {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }

    RowsetSharedPtr create_rowset_with_mutiple_segments(const TabletSharedPtr& tablet,
                                                        const vector<vector<int64_t>>& keys_by_segment,
                                                        Column* one_delete = nullptr, bool empty = false,
                                                        bool has_merge_condition = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = OVERLAP_UNKNOWN;
        if (has_merge_condition) {
            writer_context.merge_condition = "v2";
        }
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        if (empty) {
            return *writer->build();
        }
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        for (int i = 0; i < keys_by_segment.size(); i++) {
            auto chunk = ChunkHelper::new_chunk(schema, keys_by_segment[i].size());
            auto& cols = chunk->columns();
            for (int64_t key : keys_by_segment[i]) {
                if (schema.num_key_fields() == 1) {
                    cols[0]->append_datum(Datum(key));
                } else {
                    cols[0]->append_datum(Datum(key));
                    string v = fmt::to_string(key * 234234342345);
                    cols[1]->append_datum(Datum(Slice(v)));
                    cols[2]->append_datum(Datum((int32_t)key));
                }
                int vcol_start = schema.num_key_fields();
                cols[vcol_start]->append_datum(Datum((int16_t)(key % 100 + 1)));
                if (cols[vcol_start + 1]->is_binary()) {
                    string v = fmt::to_string(key % 1000 + 2);
                    cols[vcol_start + 1]->append_datum(Datum(Slice(v)));
                } else {
                    cols[vcol_start + 1]->append_datum(Datum((int32_t)(key % 1000 + 2)));
                }
            }
            if (one_delete == nullptr && !keys_by_segment[i].empty()) {
                CHECK_OK(writer->flush_chunk(*chunk));
            } else if (one_delete == nullptr) {
                CHECK_OK(writer->flush());
            } else if (one_delete != nullptr) {
                CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
            }
        }
        return *writer->build();
    }

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes,
                                          const std::shared_ptr<TabletSchema>& partial_schema) {
        // create partial rowset
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = partial_schema;
        writer_context.referenced_column_ids = column_indexes;
        writer_context.full_tablet_schema = tablet->tablet_schema();
        writer_context.is_partial_update = true;
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(partial_schema);

        if (keys.size() > 0) {
            auto chunk = ChunkHelper::new_chunk(schema, keys.size());
            EXPECT_TRUE(2 == chunk->num_columns());
            auto& cols = chunk->columns();
            for (long key : keys) {
                cols[0]->append_datum(Datum(key));
                cols[1]->append_datum(Datum((int16_t)(key % 100 + 3)));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

    RowsetSharedPtr create_rowsets(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                   std::size_t max_rows_per_segment) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        for (std::size_t written_rows = 0; written_rows < keys.size(); written_rows += max_rows_per_segment) {
            auto chunk = ChunkHelper::new_chunk(schema, max_rows_per_segment);
            auto& cols = chunk->columns();
            for (size_t i = 0; i < max_rows_per_segment; i++) {
                cols[0]->append_datum(Datum(keys[written_rows + i]));
                cols[1]->append_datum(Datum((int16_t)(keys[written_rows + i] % 100 + 1)));
                cols[2]->append_datum(Datum((int32_t)(keys[written_rows + i] % 1000 + 2)));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

    RowsetSharedPtr create_rowset_schema_change_sort_key(const TabletSharedPtr& tablet, const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        const auto nkeys = keys.size();
        auto chunk = ChunkHelper::new_chunk(schema, nkeys);
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(nkeys - 1 - key)));
            cols[2]->append_datum(Datum((int32_t)(key)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    StatusOr<RowsetSharedPtr> create_rowset_column_with_row(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                                            bool large_var_column = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        const auto nkeys = keys.size();
        const auto& column = tablet->thread_safe_get_tablet_schema()->columns().back();
        std::vector<ColumnId> cids(tablet->thread_safe_get_tablet_schema()->num_columns() - 1);
        if (column.name() == Schema::FULL_ROW_COLUMN) {
            for (int i = 0; i < tablet->thread_safe_get_tablet_schema()->num_columns() - 1; i++) {
                cids[i] = i;
            }
        }
        auto schema_without_full_row_column = std::make_unique<Schema>(&schema, cids);
        auto chunk = ChunkHelper::new_chunk(*schema_without_full_row_column, nkeys);
        string varchar_value;
        if (large_var_column) {
            varchar_value = std::string(1024 * 1024, 'a');
        } else {
            varchar_value = std::string(1024, 'a');
        }
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(nkeys - 1 - key)));
            cols[2]->append_datum(Datum(Slice(varchar_value)));
        }
        RETURN_IF_ERROR(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    RowsetSharedPtr create_rowset_sort_key_error_encode_case(const TabletSharedPtr& tablet,
                                                             const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        const auto nkeys = keys.size();
        auto chunk = ChunkHelper::new_chunk(schema, nkeys);
        auto& cols = chunk->columns();
        for (auto i = 0; i < nkeys; ++i) {
            cols[0]->append_datum(Datum(keys[i]));
            cols[1]->append_datum(Datum((int16_t)1));
            cols[2]->append_datum(Datum((int32_t)(keys[nkeys - 1 - i])));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    RowsetSharedPtr create_nullable_sort_key_rowset(const TabletSharedPtr& tablet,
                                                    const vector<vector<int64_t>>& all_cols) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        const auto keys_size = all_cols[0].size();
        auto chunk = ChunkHelper::new_chunk(schema, keys_size);
        auto& cols = chunk->columns();
        for (auto i = 0; i < keys_size; ++i) {
            append_datum_func(cols[0], static_cast<int64_t>(all_cols[0][i]));
            append_datum_func(cols[1], static_cast<int16_t>(all_cols[1][i]));
            append_datum_func(cols[2], static_cast<int32_t>(all_cols[2][i]));
        }

        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, bool multi_column_pk = false,
                                  int64_t schema_id = 0, int32_t schema_version = 0) {
        srand(GetCurrentTimeMicros());
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        request.tablet_schema.__set_id(schema_id);
        request.tablet_schema.__set_schema_version(schema_version);

        if (multi_column_pk) {
            TColumn pk1;
            pk1.column_name = "pk1_bigint";
            pk1.__set_is_key(true);
            pk1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(pk1);
            TColumn pk2;
            pk2.column_name = "pk2_varchar";
            pk2.__set_is_key(true);
            pk2.column_type.type = TPrimitiveType::VARCHAR;
            pk2.column_type.len = 128;
            request.tablet_schema.columns.push_back(pk2);
            TColumn pk3;
            pk3.column_name = "pk3_int";
            pk3.__set_is_key(true);
            pk3.column_type.type = TPrimitiveType::INT;
            request.tablet_schema.columns.push_back(pk3);
        } else {
            TColumn k1;
            k1.column_name = "pk";
            k1.__set_is_key(true);
            k1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(k1);
        }

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_with_sort_key(int64_t tablet_id, int32_t schema_hash,
                                                std::vector<int32_t> sort_key_idxes) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        request.tablet_schema.sort_key_idxes = sort_key_idxes;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_with_nullable_sort_key(int64_t tablet_id, int32_t schema_hash,
                                                         std::vector<int32_t> sort_key_idxes) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        request.tablet_schema.sort_key_idxes = sort_key_idxes;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.__set_is_allow_null(true);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.__set_is_allow_null(true);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_column_with_row(int64_t tablet_id, int32_t schema_hash,
                                                  bool multi_column_pk = false) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        if (multi_column_pk) {
            TColumn pk1;
            pk1.column_name = "pk1_bigint";
            pk1.__set_is_key(true);
            pk1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(pk1);
            TColumn pk2;
            pk2.column_name = "pk2_varchar";
            pk2.__set_is_key(true);
            pk2.column_type.type = TPrimitiveType::VARCHAR;
            pk2.column_type.len = 128;
            request.tablet_schema.columns.push_back(pk2);
            TColumn pk3;
            pk3.column_name = "pk3_int";
            pk3.__set_is_key(true);
            pk3.column_type.type = TPrimitiveType::INT;
            request.tablet_schema.columns.push_back(pk3);
        } else {
            TColumn k1;
            k1.column_name = "pk";
            k1.__set_is_key(true);
            k1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(k1);
        }

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::VARCHAR;
        k3.column_type.len = TypeDescriptor::MAX_VARCHAR_LENGTH;
        request.tablet_schema.columns.push_back(k3);

        TColumn row;
        row.column_name = Schema::FULL_ROW_COLUMN;
        TColumnType ctype;
        ctype.__set_type(TPrimitiveType::VARCHAR);
        ctype.__set_len(TypeDescriptor::MAX_VARCHAR_LENGTH);
        row.__set_column_type(ctype);
        row.__set_aggregation_type(TAggregationType::REPLACE);
        row.__set_is_allow_null(false);
        row.__set_default_value("");
        request.tablet_schema.columns.push_back(row);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet2(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);

        TColumn k4;
        k4.column_name = "v3";
        k4.__set_is_key(false);
        k4.column_type.type = TPrimitiveType::INT;
        k4.__set_default_value("1");
        request.tablet_schema.columns.push_back(k4);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_to_schema_change(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::VARCHAR;
        k3.column_type.len = 128;
        request.tablet_schema.columns.push_back(k3);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_to_add_generated_column(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);

        TColumn k4;
        k4.column_name = "newcol";
        k4.__set_is_key(false);
        k4.__set_is_allow_null(true);
        k4.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k4);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void SetUp() override {
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        config::enable_pk_size_tiered_compaction_strategy = false;
    }

    void TearDown() override {
        if (_tablet2) {
            (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet2->tablet_id());
            _tablet2.reset();
        }
        if (_tablet) {
            (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
        config::enable_pk_size_tiered_compaction_strategy = true;
    }

    static Status full_clone(const TabletSharedPtr& source_tablet, int clone_version,
                             const TabletSharedPtr& dest_tablet) {
        auto snapshot_dir = SnapshotManager::instance()->snapshot_full(source_tablet, clone_version, 3600);
        CHECK(snapshot_dir.ok()) << snapshot_dir.status();

        DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

        auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, *snapshot_dir);
        auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
        CHECK(snapshot_meta.ok()) << snapshot_meta.status();

        RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir));

        std::set<std::string> files;
        auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
        CHECK(st.ok()) << st;
        files.erase("meta");

        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = dest_tablet->schema_hash_path() + "/" + f;
            st = FileSystem::Default()->link_file(src, dst);
            if (st.ok()) {
                LOG(INFO) << "Linked " << src << " to " << dst;
            } else if (st.is_already_exist()) {
                LOG(INFO) << dst << " already exist";
            } else {
                return st;
            }
        }
        // Pretend that source_tablet is a peer replica of dest_tablet
        snapshot_meta->tablet_meta().set_tablet_id(dest_tablet->tablet_id());
        snapshot_meta->tablet_meta().set_schema_hash(dest_tablet->schema_hash());
        for (auto& rm : snapshot_meta->rowset_metas()) {
            rm.set_tablet_id(dest_tablet->tablet_id());
        }

        st = dest_tablet->updates()->load_snapshot(*snapshot_meta);
        dest_tablet->updates()->remove_expired_versions(time(nullptr));
        return st;
    }

    static StatusOr<TabletSharedPtr> clone_a_new_replica(const TabletSharedPtr& source_tablet, int64_t new_tablet_id) {
        auto clone_version = source_tablet->max_version().second;
        auto snapshot_dir = SnapshotManager::instance()->snapshot_full(source_tablet, clone_version, 3600);
        CHECK(snapshot_dir.ok()) << snapshot_dir.status();

        DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

        auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, *snapshot_dir);
        auto meta_file = meta_dir + "/meta";
        auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_file);
        CHECK(snapshot_meta.ok()) << snapshot_meta.status();

        // Assign a new tablet_id and overwrite the meta file.
        snapshot_meta->tablet_meta().set_tablet_id(new_tablet_id);
        CHECK(snapshot_meta->serialize_to_file(meta_file).ok());

        RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir));

        auto store = source_tablet->data_dir();
        auto new_schema_hash = source_tablet->schema_hash();
        std::string new_tablet_path = store->path() + DATA_PREFIX;
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(source_tablet->shard_id()));
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(new_tablet_id));
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(new_schema_hash));
        CHECK(std::filesystem::create_directories(new_tablet_path));

        std::set<std::string> files;
        CHECK(fs::list_dirs_files(meta_dir, nullptr, &files).ok());
        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = new_tablet_path + "/" + f;
            Status st = FileSystem::Default()->link_file(src, dst);
            if (st.ok()) {
                LOG(INFO) << "Linked " << src << " to " << dst;
            } else if (st.is_already_exist()) {
                LOG(INFO) << dst << " already exist";
            } else {
                return st;
            }
        }

        auto tablet_manager = StorageEngine::instance()->tablet_manager();
        auto st = tablet_manager->create_tablet_from_meta_snapshot(store, new_tablet_id, new_schema_hash,
                                                                   new_tablet_path);
        CHECK(st.ok()) << st;
        return tablet_manager->get_tablet(new_tablet_id, false);
    }

    void test_writeread(bool enable_persistent_index);
    void test_writeread_with_delete(bool enable_persistent_index);
    void test_noncontinous_commit(bool enable_persistent_index);
    void test_noncontinous_meta_save_load(bool enable_persistent_index);
    void test_save_meta(bool enable_persistent_index);
    void test_load_from_pb(bool enable_persistent_index);
    void test_remove_expired_versions(bool enable_persistent_index);
    void test_apply(bool enable_persistent_index, bool has_merge_condition);
    void test_apply_breakpoint_check(bool enable_persistent_index);
    void test_condition_update_apply(bool enable_persistent_index);
    void test_concurrent_write_read_and_gc(bool enable_persistent_index);
    void test_compaction_score_not_enough(bool enable_persistent_index);
    void test_compaction_score_enough_duplicate(bool enable_persistent_index);
    void test_compaction_score_enough_normal(bool enable_persistent_index);
    void test_horizontal_compaction(bool enable_persistent_index, bool show_status = false);
    void test_vertical_compaction(bool enable_persistent_index);
    void test_horizontal_compaction_with_rows_mapper(bool enable_persistent_index);
    void test_vertical_compaction_with_rows_mapper(bool enable_persistent_index);
    void test_compaction_with_empty_rowset(bool enable_persistent_index, bool vertical, bool multi_column_pk);
    void test_link_from(bool enable_persistent_index);
    void test_convert_from(bool enable_persistent_index);
    void test_convert_from_with_pending(bool enable_persistent_index);
    void test_convert_from_with_mutiple_segment(bool enable_persistent_index);
    void test_reorder_from(bool enable_persistent_index);
    void test_load_snapshot_incremental(bool enable_persistent_index);
    void test_load_snapshot_incremental_ignore_already_committed_version(bool enable_persistent_index);
    void test_load_snapshot_incremental_mismatched_tablet_id(bool enable_persistent_index);
    void test_load_snapshot_incremental_data_file_not_exist(bool enable_persistent_index);
    void test_load_snapshot_incremental_incorrect_version(bool enable_persistent_index);
    void test_load_snapshot_incremental_with_partial_rowset_old(bool enable_persistent_index);
    void test_load_snapshot_incremental_with_partial_rowset_new(bool enable_persistent_index,
                                                                PartialUpdateCloneCase update_case);
    void test_load_snapshot_primary(int64_t num_version, const std::vector<uint64_t>& holes);
    void test_load_snapshot_primary(int64_t max_version, const std::vector<uint64_t>& holes,
                                    bool enable_persistent_index);
    void test_load_snapshot_full(bool enable_persistent_index);
    void test_load_snapshot_full_file_not_exist(bool enable_persistent_index);
    void test_load_snapshot_full_mismatched_tablet_id(bool enable_persistent_index);
    void test_issue_4193(bool enable_persistent_index);
    void test_issue_4181(bool enable_persistent_index);
    void test_snapshot_with_empty_rowset(bool enable_persistent_index);
    void test_get_column_values(bool enable_persistent_index);
    void test_get_missing_version_ranges(const std::vector<int64_t>& versions,
                                         const std::vector<int64_t>& expected_missing_ranges);
    void test_get_rowsets_for_incremental_snapshot(const std::vector<int64_t>& versions,
                                                   const std::vector<int64_t>& missing_ranges,
                                                   const std::vector<int64_t>& expect_rowset_versions, bool gc,
                                                   bool expect_error);

    void tablets_prepare(const TabletSharedPtr& tablet0, const TabletSharedPtr& tablet1,
                         std::vector<int32_t>& column_indexes, const std::shared_ptr<TabletSchema>& partial_schema);
    void snapshot_prepare(const TabletSharedPtr& tablet, const std::vector<int64_t>& delta_versions,
                          std::string* snapshot_id_path, std::string* snapshot_dir,
                          std::vector<RowsetSharedPtr>* snapshot_rowsets,
                          std::vector<RowsetMetaSharedPtr>* snapshot_rowset_metas,
                          const TabletMetaSharedPtr& snapshot_tablet_meta);
    void load_snapshot(const std::string& meta_dir, const TabletSharedPtr& tablet, SegmentFooterPB* footer);
    void test_schema_change_optimiazation_adding_generated_column(bool enable_persistent_index);
    void test_pk_dump(size_t rowset_cnt);
    void update_and_recover(bool enable_persistent_index);
    void test_recover_rowset_sorter();

protected:
    TabletSharedPtr _tablet;
    TabletSharedPtr _tablet2;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
};

ssize_t read_tablet_and_compare_schema_changed(const TabletSharedPtr& tablet, int64_t version,
                                               const vector<int64_t>& keys);

ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version);

ssize_t read_tablet_and_compare(const TabletSharedPtr& tablet, int64_t version, const vector<int64_t>& keys);

ssize_t read_tablet_and_compare_schema_changed_sort_key1(const TabletSharedPtr& tablet, int64_t version,
                                                         const vector<int64_t>& keys);

ssize_t read_tablet_and_compare_schema_changed_sort_key2(const TabletSharedPtr& tablet, int64_t version,
                                                         const vector<int64_t>& keys);

} // namespace starrocks
