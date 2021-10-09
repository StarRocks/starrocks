// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/rowset_merger.h"

#include <gtest/gtest.h>

#include "gutil/strings/substitute.h"
#include "storage/kv_store.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/empty_iterator.h"
#include "storage/vectorized/union_iterator.h"

namespace starrocks::vectorized {

class TestRowsetWriter : public RowsetWriter {
public:
    ~TestRowsetWriter() = default;

    OLAPStatus init() override { return OLAP_SUCCESS; }

    OLAPStatus add_row(const RowCursor& row) override { return OLAP_ERR_FUNC_NOT_IMPLEMENTED; }
    OLAPStatus add_row(const ContiguousRow& row) override { return OLAP_ERR_FUNC_NOT_IMPLEMENTED; }

    OLAPStatus add_chunk(const vectorized::Chunk& chunk) override { return OLAP_ERR_FUNC_NOT_IMPLEMENTED; }

    OLAPStatus flush_chunk(const vectorized::Chunk& chunk) override { return OLAP_ERR_FUNC_NOT_IMPLEMENTED; }

    OLAPStatus flush_chunk_with_deletes(const vectorized::Chunk& upserts, const vectorized::Column& deletes) override {
        return OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    OLAPStatus add_rowset(RowsetSharedPtr rowset) override { return OLAP_ERR_FUNC_NOT_IMPLEMENTED; }

    OLAPStatus add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                   const SchemaMapping& schema_mapping) override {
        return OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    RowsetSharedPtr build() override { return RowsetSharedPtr(); }

    Version version() override { return Version(); }

    int64_t num_rows() override { return all_pks->size(); }

    int64_t total_data_size() override { return 0; }

    RowsetId rowset_id() override { return RowsetId(); }

    OLAPStatus flush() override { return OLAP_SUCCESS; }

    OLAPStatus add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) {
        all_pks->append(*chunk.get_column_by_index(0), 0, chunk.num_rows());
        all_rssids.insert(all_rssids.end(), rssid.begin(), rssid.end());
        return OLAP_SUCCESS;
    }

    std::unique_ptr<Column> all_pks;
    vector<uint32_t> all_rssids;
};

class RowsetMergerTest : public testing::Test {
public:
    RowsetSharedPtr create_rowset(const vector<int64_t>& keys) {
        // TODO(cbl): test multi-segment rowsets
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = _tablet->tablet_id();
        writer_context.tablet_schema_hash = _tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = _tablet->tablet_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &_tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = vectorized::ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (size_t i = 0; i < keys.size(); i++) {
            cols[0]->append_datum(vectorized::Datum(keys[i]));
            cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 1000 + 2)));
        }
        EXPECT_EQ(OLAP_SUCCESS, writer->flush_chunk(*chunk));
        return writer->build();
    }

    void create_tablet(int64_t tablet_id, int32_t schema_hash) {
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
        ASSERT_TRUE(st.ok()) << st.to_string();
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
        ASSERT_TRUE(_tablet);
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id(), _tablet->schema_hash(),
                                                                     false);
            _tablet.reset();
        }
    }

protected:
    TabletSharedPtr _tablet;
};

static vectorized::ChunkIteratorPtr create_tablet_iterator(const TabletSharedPtr& tablet, int64_t version) {
    static OlapReaderStatistics s_stats;
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema(tablet->tablet_schema());
    vectorized::RowsetReadOptions rs_opts;
    rs_opts.is_primary_keys = true;
    rs_opts.sorted = false;
    rs_opts.version = version;
    rs_opts.meta = tablet->data_dir()->get_meta();
    rs_opts.stats = &s_stats;
    auto seg_iters = tablet->capture_segment_iterators(Version(0, version), schema, rs_opts);
    if (!seg_iters.ok()) {
        LOG(ERROR) << "read tablet failed: " << seg_iters.status().to_string();
        return nullptr;
    }
    if (seg_iters->empty()) {
        return vectorized::new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return vectorized::new_union_iterator(*seg_iters);
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
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    auto iter = create_tablet_iterator(tablet, version);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

TEST_F(RowsetMergerTest, merge) {
    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 1 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 1000 + rand() % 2000;
    LOG(INFO) << "merge test #rowset:" << num_segment << " #row:" << N << " chunk_size:" << cfg.chunk_size;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = rand() % num_segment;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }
    int64_t version = num_segment + 1;
    EXPECT_EQ(N, read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    ASSERT_TRUE(vectorized::compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());
    ASSERT_EQ(N, writer.all_pks->size());
    const int64_t* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());
    for (int64_t i = 0; i < N; i++) {
        ASSERT_EQ(i, raw_pk_array[i]);
    }
    EXPECT_EQ(rssids, writer.all_rssids);
}

TEST_F(RowsetMergerTest, merge_seq) {
    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 1 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 100 + rand() % 2000;
    // small size test
    //    const int num_segment = 3;
    //    const int N = 30;
    //    MergeConfig cfg;
    //    cfg.chunk_size = 20;
    LOG(INFO) << "seq merge test #rowset:" << num_segment << " #row:" << N << " chunk_size:" << cfg.chunk_size;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = num_segment * i / N;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }
    int64_t version = num_segment + 1;
    EXPECT_EQ(N, read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    ASSERT_TRUE(vectorized::compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());
    ASSERT_EQ(N, writer.all_pks->size());
    const int64_t* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());
    for (int64_t i = 0; i < N; i++) {
        ASSERT_EQ(i, raw_pk_array[i]);
    }
    EXPECT_EQ(rssids, writer.all_rssids);
}

} // namespace starrocks::vectorized
