// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/rowset/vectorized/segment_iterator.h"

#include <memory>
#include <string>
#include <unordered_map>

#include "common/object_pool.h"
#include "env/env_memory.h"
#include "gtest/gtest.h"
#include "runtime/mem_pool.h"
#include "storage/fs/block_manager.h"
#include "storage/fs/file_block_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/segment.h"
#include "storage/rowset/segment_v2/segment_iterator.h"
#include "storage/rowset/segment_v2/segment_writer.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/tablet_schema_helper.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "util/defer_op.h"

#define ASSERT_OK(expr)                                   \
    do {                                                  \
        Status _status = (expr);                          \
        ASSERT_TRUE(_status.ok()) << _status.to_string(); \
    } while (0)

namespace starrocks {

class SegmentIteratorTest : public ::testing::Test {
public:
    void SetUp() override {
        _env = new EnvMemory();
        _block_mgr = new fs::FileBlockManager(_env, fs::BlockManagerOptions());
        ASSERT_TRUE(_env->create_dir(kSegmentDir).ok());
        _page_cache_mem_tracker = std::make_unique<MemTracker>();
        _tablet_meta_mem_tracker = std::make_unique<MemTracker>();
        _mem_pool = std::make_unique<MemPool>();
        StoragePageCache::create_global_cache(_page_cache_mem_tracker.get(), 1000000000);
    }

    void TearDown() override {
        delete _block_mgr;
        delete _env;
        StoragePageCache::release_global_cache();
    }

    TabletSchema create_schema(const std::vector<TabletColumn>& columns, int num_short_key_columns = -1) {
        TabletSchema res;
        int num_key_columns = 0;
        for (auto& col : columns) {
            if (col.is_key()) {
                num_key_columns++;
            }
            res._cols.push_back(col);
        }
        res._num_key_columns = num_key_columns;
        res._num_short_key_columns = num_short_key_columns != -1 ? num_short_key_columns : num_key_columns;
        return res;
    }

    const std::string kSegmentDir = "/segment_test";
    EnvMemory* _env = nullptr;
    fs::FileBlockManager* _block_mgr = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _tablet_meta_mem_tracker = nullptr;
    std::unique_ptr<MemPool> _mem_pool;
};

TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSet) {
    const int slice_num = 64;
    std::string prefix = "lowcard-";
    std::vector<std::string> values;
    for (int i = 0; i < slice_num; ++i) {
        values.push_back(prefix + std::to_string(i));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.push_back(data);
    }

    TabletColumn c1 = create_int_key(1);
    TabletColumn c2 = create_with_default_value<OLAP_FIELD_TYPE_VARCHAR>("");

    TabletSchema tablet_schema = create_schema({c1, c2});

    segment_v2::SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::string file_name = kSegmentDir + "/low_card_cols";
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({file_name});
    ASSERT_TRUE(_block_mgr->create_block(wblock_opts, &wblock).ok());

    segment_v2::SegmentWriter writer(std::move(wblock), 0, &tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;

    {
        // col0
        ASSERT_TRUE(writer.init(10).ok());
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema);
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, chunk_size);

        RowCursor row;
        auto olap_st = row.init(tablet_schema);
        ASSERT_EQ(OLAP_SUCCESS, olap_st);

        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            // chunk->reset();
            for (auto j = 0; j < chunk_size; ++j) {
                if (i * chunk_size + j >= num_rows) {
                    break;
                }
                auto cell0 = row.cell(0);
                *reinterpret_cast<int*>(cell0.mutable_cell_ptr()) = static_cast<int32_t>(i * chunk_size + j);
                auto cell1 = row.cell(1);
                Slice src = data_strs[j % slice_num];
                auto* slice = reinterpret_cast<Slice*>(cell1.mutable_cell_ptr());
                slice->data = (char*)_mem_pool->allocate(src.size);
                slice->size = src.size;
                memcpy(slice->data, src.data, slice->size);
                writer.append_row(row);
            }
            // ASSERT_TRUE(writer.append_chunk(*chunk).ok());
        }
        uint64_t file_size, index_size;
        ASSERT_OK(writer.finalize(&file_size, &index_size));
    }

    auto segment = *segment_v2::Segment::open(_tablet_meta_mem_tracker.get(), _block_mgr, file_name, 0, &tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    vectorized::SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.block_mgr = _block_mgr;
    seg_options.stats = &stats;

    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema);
    auto res = segment->new_iterator(schema, seg_options);

    vectorized::Schema vec_schema;
    vec_schema.append(std::make_shared<vectorized::Field>(0, "c1", OLAP_FIELD_TYPE_INT, -1, -1, false));
    vec_schema.append(std::make_shared<vectorized::Field>(1, "c2", OLAP_FIELD_TYPE_VARCHAR, -1, -1, false));

    ObjectPool pool;
    vectorized::SegmentReadOptions seg_opts;
    seg_opts.block_mgr = _block_mgr;
    seg_opts.stats = &stats;

    auto* con = pool.add(new vectorized::ConjunctivePredicates());
    auto type_varchar = ScalarTypeInfoResolver::instance()->get_type_info(OLAP_FIELD_TYPE_VARCHAR);
    con->add(pool.add(vectorized::new_column_ge_predicate(type_varchar, 1, Slice(values[8]))));
    seg_opts.delete_predicates.add(*con);
    // seg_opts.predicates[1].push_back(vectorized::new_column_le_predicate(type_varchar, 1, Slice(values[8])));

    vectorized::ColumnIdToGlobalDictMap dict_map;
    vectorized::GlobalDictMap g_dict;
    for (int i = 0; i < 8; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    chunk_iter->init_encoded_schema(dict_map);

    auto res_chunk = vectorized::ChunkHelper::new_chunk(chunk_iter->encoded_schema(), chunk_size);
    ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).ok());
    res_chunk->reset();
    ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).ok());
    res_chunk->reset();
    ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).ok());
    res_chunk->reset();
}

} // namespace starrocks
