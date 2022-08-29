// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset_update_state.h"

#include <gtest/gtest.h>

#include <functional>
#include <iostream>
#include <memory>

#include "column/datum_tuple.h"
#include "env/env_memory.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "storage/vectorized/empty_iterator.h"
#include "storage/vectorized/tablet_reader.h"
#include "storage/vectorized/tablet_reader_params.h"
#include "storage/vectorized/union_iterator.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

class RowsetUpdateStateTest : public ::testing::Test {
public:
    void SetUp() override {
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _metadata_mem_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  vectorized::Column* one_delete = nullptr) {
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (size_t i = 0; i < keys.size(); i++) {
            cols[0]->append_datum(vectorized::Datum(keys[i]));
            cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 1000 + 2)));
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

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 6;
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
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

protected:
    TabletSharedPtr _tablet;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
};

static vectorized::ChunkIteratorPtr create_tablet_iterator(vectorized::TabletReader& reader,
                                                           vectorized::Schema& schema) {
    vectorized::TabletReaderParams params;
    if (!reader.prepare().ok()) {
        LOG(ERROR) << "reader prepare failed";
        return nullptr;
    }
    std::vector<ChunkIteratorPtr> seg_iters;
    if (!reader.get_segment_iterators(params, &seg_iters).ok()) {
        LOG(ERROR) << "reader get segment iterators fail";
        return nullptr;
    }
    if (seg_iters.empty()) {
        return vectorized::new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return vectorized::new_union_iterator(seg_iters);
}

static ssize_t read_until_eof(const vectorized::ChunkIteratorPtr& iter) {
    auto chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            count += chunk->num_rows();
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

TEST_F(RowsetUpdateStateTest, prepare_partial_update_states) {
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(10);
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys));
    }
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    for (int i = 0; i < rowsets.size(); i++) {
        auto version = i + 2;
        auto st = _tablet->rowset_commit(version, rowsets[i]);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Ensure that there is at most one thread doing the version apply job.
        ASSERT_LE(pool->num_threads(), 1);
        ASSERT_EQ(version, _tablet->updates()->max_version());
        ASSERT_EQ(version, _tablet->updates()->version_history_count());
    }
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size()));

    // create partial rowset second
    RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
    RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_id = _tablet->tablet_id();
    writer_context.tablet_schema_hash = _tablet->schema_hash();
    writer_context.partition_id = 0;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = COMMITTED;
    std::vector<int32_t> column_indexes = {0, 1};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
    writer_context.partial_update_tablet_schema = partial_schema;
    writer_context.referenced_column_ids = column_indexes;
    writer_context.tablet_schema = writer_context.partial_update_tablet_schema.get();
    writer_context.version.first = 0;
    writer_context.version.second = 0;
    writer_context.segments_overlap = NONOVERLAPPING;
    std::unique_ptr<RowsetWriter> writer;
    EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*partial_schema.get());

    auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
    ASSERT_EQ(2, chunk->num_columns());
    auto& cols = chunk->columns();
    for (size_t i = 0; i < keys.size(); i++) {
        cols[0]->append_datum(vectorized::Datum(keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 3)));
    }
    CHECK_OK(writer->flush_chunk(*chunk));
    RowsetSharedPtr partial_rowset = *writer->build();

    // check data of write column
    RowsetUpdateState state;
    state.load(_tablet.get(), partial_rowset.get());
    const std::vector<PartialUpdateState>& parital_update_states = state.parital_update_states();
    ASSERT_EQ(parital_update_states.size(), 1);
    ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(parital_update_states[0].write_columns.size(), 1);
    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ((int32_t)(keys[i] % 1000 + 2), parital_update_states[0].write_columns[0]->get(i).get_int32());
    }
}

} // namespace starrocks
