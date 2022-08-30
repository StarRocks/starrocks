// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/memtable.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "fs/fs_util.h"
#include "gutil/strings/split.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/memtable_rowset_writer_sink.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "testutil/assert.h"

namespace starrocks::vectorized {

using namespace std;

static shared_ptr<TabletSchema> create_tablet_schema(const string& desc, int nkey, KeysType key_type) {
    TabletSchemaPB tspb;
    std::vector<std::string> cs = strings::Split(desc, ",", strings::SkipWhitespace());
    uint32_t cid = 0;
    for (std::string& c : cs) {
        ColumnPB* cpb = tspb.add_column();
        std::vector<std::string> fs = strings::Split(c, " ", strings::SkipWhitespace());
        if (fs.size() < 2) {
            CHECK(false) << "create_tablet_schema bad schema desc";
        }
        cpb->set_is_key(cid < nkey);
        if (cid < nkey) {
            cpb->set_aggregation("none");
        } else {
            cpb->set_aggregation("replace");
        }
        cpb->set_unique_id(cid++);
        cpb->set_name(fs[0]);
        cpb->set_type(fs[1]);
        if (fs[1] == "varchar") {
            cpb->set_length(65535);
        }
        if (fs.size() == 3 && fs[2] == "null") {
            cpb->set_is_nullable(true);
        }
    }
    tspb.set_keys_type(key_type);
    tspb.set_next_column_unique_id(cid);
    tspb.set_num_short_key_columns(nkey);
    return std::make_shared<TabletSchema>(tspb);
}

static unique_ptr<Schema> create_schema(const string& desc, int nkey) {
    unique_ptr<Schema> ret;
    Fields fields;
    std::vector<std::string> cs = strings::Split(desc, ",", strings::SkipWhitespace());
    for (int i = 0; i < cs.size(); i++) {
        auto& c = cs[i];
        std::vector<std::string> fs = strings::Split(c, " ", strings::SkipWhitespace());
        if (fs.size() < 2) {
            CHECK(false) << "create_tablet_schema bad schema desc";
        }
        ColumnId cid = i;
        string name = fs[0];
        FieldType type = OLAP_FIELD_TYPE_UNKNOWN;
        if (fs[1] == "boolean") {
            type = OLAP_FIELD_TYPE_BOOL;
        } else if (fs[1] == "tinyint") {
            type = OLAP_FIELD_TYPE_TINYINT;
        } else if (fs[1] == "smallint") {
            type = OLAP_FIELD_TYPE_SMALLINT;
        } else if (fs[1] == "int") {
            type = OLAP_FIELD_TYPE_INT;
        } else if (fs[1] == "bigint") {
            type = OLAP_FIELD_TYPE_BIGINT;
        } else if (fs[1] == "float") {
            type = OLAP_FIELD_TYPE_FLOAT;
        } else if (fs[1] == "double") {
            type = OLAP_FIELD_TYPE_DOUBLE;
        } else if (fs[1] == "varchar") {
            type = OLAP_FIELD_TYPE_VARCHAR;
        } else {
            CHECK(false) << "create_tuple_desc_slots type not support";
        }
        bool nullable = false;
        if (fs.size() == 3 && fs[2] == "null") {
            nullable = true;
        }
        auto fd = new Field(cid, name, type, nullable);
        fd->set_is_key(i < nkey);
        fd->set_aggregate_method(i < nkey ? OLAP_FIELD_AGGREGATION_NONE : OLAP_FIELD_AGGREGATION_REPLACE);
        fields.emplace_back(fd);
    }
    ret.reset(new Schema(std::move(fields)));
    return ret;
}

static const std::vector<SlotDescriptor*>* create_tuple_desc_slots(const string& desc, ObjectPool& pool) {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    std::vector<std::string> cs = strings::Split(desc, ",", strings::SkipWhitespace());
    for (int i = 0; i < cs.size(); i++) {
        auto& c = cs[i];
        std::vector<std::string> fs = strings::Split(c, " ", strings::SkipWhitespace());
        if (fs.size() < 2) {
            CHECK(false) << "create_tuple_desc_slots bad desc";
        }
        PrimitiveType type = INVALID_TYPE;
        if (fs[1] == "boolean") {
            type = TYPE_BOOLEAN;
        } else if (fs[1] == "tinyint") {
            type = TYPE_TINYINT;
        } else if (fs[1] == "smallint") {
            type = TYPE_SMALLINT;
        } else if (fs[1] == "int") {
            type = TYPE_INT;
        } else if (fs[1] == "bigint") {
            type = TYPE_BIGINT;
        } else if (fs[1] == "float") {
            type = TYPE_FLOAT;
        } else if (fs[1] == "double") {
            type = TYPE_DOUBLE;
        } else if (fs[1] == "varchar") {
            type = TYPE_VARCHAR;
        } else {
            CHECK(false) << "create_tuple_desc_slots type not support";
        }
        bool nullable = false;
        if (fs.size() == 3 && fs[2] == "null") {
            nullable = true;
        }
        tuple_builder.add_slot(TSlotDescriptorBuilder().column_name(fs[0]).type(type).nullable(nullable).build());
    }
    tuple_builder.build(&dtb);
    TDescriptorTable tdesc_tbl = dtb.desc_tbl();
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&pool, tdesc_tbl, &desc_tbl, config::vector_chunk_size);
    return &(desc_tbl->get_tuple_descriptor(0)->slots());
}

static shared_ptr<Chunk> gen_chunk(const std::vector<SlotDescriptor*>& slots, size_t size) {
    shared_ptr<Chunk> ret = ChunkHelper::new_chunk(slots, size);
    auto& cols = ret->columns();
    for (int ci = 0; ci < cols.size(); ci++) {
        ColumnPtr& c = cols[ci];
        Datum v;
        string strv;
        for (size_t i = 0; i < size; i++) {
            auto type = slots[ci]->type().type;
            if (type == TYPE_BOOLEAN) {
                v.set_uint8(i % 2);
            } else if (type == TYPE_TINYINT) {
                v.set_int8((i + 1) % 128);
            } else if (type == TYPE_SMALLINT) {
                v.set_int16((i + 2) % 65535);
            } else if (type == TYPE_INT) {
                v.set_int32(i + 3);
            } else if (type == TYPE_BIGINT) {
                v.set_int16(i * 3);
            } else if (type == TYPE_FLOAT) {
                v.set_float(i * 4);
            } else if (type == TYPE_DOUBLE) {
                v.set_double(i * 5);
            } else if (type == TYPE_VARCHAR) {
                strv = StringPrintf("str%d", ci);
                v.set_slice(strv);
            } else {
                CHECK(false) << "gen_chunk type not supported";
            }
            c->append_datum(v);
        }
    }
    return ret;
}

class MemTableTest : public ::testing::Test {
public:
    void MySetUp(const string& schema_desc, const string& slot_desc, int nkey, KeysType ktype, const string& root) {
        _root_path = root;
        fs::remove_all(_root_path);
        fs::create_directories(_root_path);
        _mem_tracker.reset(new MemTracker(-1, "root"));
        _schema = create_tablet_schema(schema_desc, nkey, ktype);
        _slots = create_tuple_desc_slots(slot_desc, _obj_pool);
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id;
        rowset_id.init(rand() % 1000000000);
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = rand() % 1000000;
        writer_context.tablet_schema_hash = 1111;
        writer_context.partition_id = 10;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = _root_path;
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = _schema.get();
        writer_context.version.first = 10;
        writer_context.version.second = 10;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &_writer).ok());
        _mem_table_sink.reset(new MemTableRowsetWriterSink(_writer.get()));
        _mem_table.reset(new MemTable(1, _schema.get(), _slots, _mem_table_sink.get(), _mem_tracker.get()));
    }

    void TearDown() override {
        LOG(INFO) << "remove dir " << _root_path;
        fs::remove_all(_root_path);
    }

    std::string _root_path;

    ObjectPool _obj_pool;
    unique_ptr<MemTracker> _mem_tracker;
    shared_ptr<TabletSchema> _schema;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    unique_ptr<RowsetWriter> _writer;
    unique_ptr<MemTable> _mem_table;
    unique_ptr<MemTableRowsetWriterSink> _mem_table_sink;
};

TEST_F(MemTableTest, testDupKeysInsertFlushRead) {
    const string path = "./ut_dir/MemTableTest_testDupKeysInsertFlushRead";
    MySetUp("pk int,name varchar,pv int", "pk int,name varchar,pv int", 1, KeysType::DUP_KEYS, path);
    const size_t n = 3000;
    auto pchunk = gen_chunk(*_slots, n);
    vector<uint32_t> indexes;
    indexes.reserve(n);
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    std::random_shuffle(indexes.begin(), indexes.end());
    _mem_table->insert(*pchunk, indexes.data(), 0, indexes.size());
    ASSERT_TRUE(_mem_table->finalize().ok());
    ASSERT_OK(_mem_table->flush());
    RowsetSharedPtr rowset = *_writer->build();
    unique_ptr<Schema> read_schema = create_schema("pk int", 1);
    OlapReaderStatistics stats;
    vectorized::RowsetReadOptions rs_opts;
    rs_opts.sorted = false;
    rs_opts.use_page_cache = false;
    rs_opts.stats = &stats;
    auto itr = rowset->new_iterator(*read_schema, rs_opts);
    ASSERT_TRUE(itr.ok()) << itr.status().to_string();
    std::shared_ptr<vectorized::Chunk> chunk = vectorized::ChunkHelper::new_chunk(*read_schema, 4096);
    size_t pkey_read = 0;
    while (true) {
        Status st = (*itr)->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        auto column = chunk->get_column_by_name("pk");
        int last_value = 0;
        for (size_t i = 0; i < column->size(); i++) {
            int new_value = column->get(i).get_int32();
            ASSERT_LE(last_value, new_value);
            last_value = new_value;
        }
        pkey_read += chunk->num_rows();
        chunk->reset();
    }
    ASSERT_EQ(n, pkey_read);
}

TEST_F(MemTableTest, testUniqKeysInsertFlushRead) {
    const string path = "./ut_dir/MemTableTest_testUniqKeysInsertFlushRead";
    MySetUp("pk int,name varchar,pv int", "pk int,name varchar,pv int", 1, KeysType::UNIQUE_KEYS, path);
    const size_t n = 1000;
    auto pchunk = gen_chunk(*_slots, n);
    vector<uint32_t> indexes;
    indexes.reserve(2 * n);
    // double input data, then test uniq key's deduplicate effect
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    std::random_shuffle(indexes.begin(), indexes.end());
    _mem_table->insert(*pchunk, indexes.data(), 0, indexes.size());
    ASSERT_TRUE(_mem_table->finalize().ok());
    ASSERT_OK(_mem_table->flush());
    RowsetSharedPtr rowset = *_writer->build();
    unique_ptr<Schema> read_schema = create_schema("pk int", 1);
    OlapReaderStatistics stats;
    vectorized::RowsetReadOptions rs_opts;
    rs_opts.sorted = false;
    rs_opts.use_page_cache = false;
    rs_opts.stats = &stats;
    auto itr = rowset->new_iterator(*read_schema, rs_opts);
    std::shared_ptr<vectorized::Chunk> chunk = vectorized::ChunkHelper::new_chunk(*read_schema, 4096);
    size_t pkey_read = 0;
    while (true) {
        Status st = (*itr)->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        auto column = chunk->get_column_by_name("pk");
        int last_value = 0;
        for (size_t i = 0; i < column->size(); i++) {
            int new_value = column->get(i).get_int32();
            ASSERT_LE(last_value, new_value);
            last_value = new_value;
        }
        pkey_read += chunk->num_rows();
        chunk->reset();
    }
    ASSERT_EQ(n, pkey_read);
}

TEST_F(MemTableTest, testPrimaryKeysWithDeletes) {
    const string path = "./ut_dir/MemTableTest_testPrimaryKeysWithDeletes";
    MySetUp("pk bigint,v1 int", "pk bigint,v1 int,__op tinyint", 1, KeysType::PRIMARY_KEYS, path);
    const size_t n = 1000;
    shared_ptr<Chunk> chunk = ChunkHelper::new_chunk(*_slots, n);
    for (int i = 0; i < n; i++) {
        Datum v;
        v.set_int64(i);
        chunk->get_column_by_index(0)->append_datum(v);
        v.set_int32(i * 3);
        chunk->get_column_by_index(1)->append_datum(v);
        v.set_int8(i % 5 == 0 ? TOpType::DELETE : TOpType::UPSERT);
        chunk->get_column_by_index(2)->append_datum(v);
    }
    vector<uint32_t> indexes;
    indexes.reserve(n);
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    std::random_shuffle(indexes.begin(), indexes.end());
    _mem_table->insert(*chunk, indexes.data(), 0, indexes.size());
    ASSERT_TRUE(_mem_table->finalize().ok());
    ASSERT_OK(_mem_table->flush());
    RowsetSharedPtr rowset = *_writer->build();
    EXPECT_EQ(1, rowset->rowset_meta()->get_num_delete_files());
}

TEST_F(MemTableTest, testPrimaryKeysSizeLimitSinglePK) {
    const string path = "./ut_dir/MemTableTest_testPrimaryKeysSizeLimitSinglePK";
    MySetUp("pk varchar,v1 int", "pk varchar,v1 int,__op tinyint", 1, KeysType::PRIMARY_KEYS, path);
    const size_t n = 1000;
    shared_ptr<Chunk> chunk = ChunkHelper::new_chunk(*_slots, n);
    string tmpstr(128, 's');
    tmpstr[tmpstr.size() - 1] = '\0';
    for (int i = 0; i < n; i++) {
        Datum v;
        v.set_slice(tmpstr);
        chunk->get_column_by_index(0)->append_datum(v);
        v.set_int32(i * 3);
        chunk->get_column_by_index(1)->append_datum(v);
        v.set_int8(i % 5 == 0 ? TOpType::DELETE : TOpType::UPSERT);
        chunk->get_column_by_index(2)->append_datum(v);
    }
    vector<uint32_t> indexes;
    indexes.reserve(n);
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    std::random_shuffle(indexes.begin(), indexes.end());
    _mem_table->insert(*chunk, indexes.data(), 0, indexes.size());
    ASSERT_TRUE(_mem_table->finalize().ok());
}

TEST_F(MemTableTest, testPrimaryKeysSizeLimitCompositePK) {
    const string path = "./ut_dir/MemTableTest_testPrimaryKeysSizeLimitCompositePK";
    MySetUp("pk int, pk varchar, pk smallint, pk boolean,v1 int",
            "pk int, pk varchar, pk smallint, pk boolean ,v1 int,__op tinyint", 4, KeysType::PRIMARY_KEYS, path);
    const size_t n = 1000;
    shared_ptr<Chunk> chunk = ChunkHelper::new_chunk(*_slots, n);
    string tmpstr(121, 's');
    tmpstr[tmpstr.size() - 1] = '\0';
    for (int i = 0; i < n; i++) {
        Datum v;
        v.set_int32(42);
        chunk->get_column_by_index(0)->append_datum(v);
        v.set_slice(tmpstr);
        chunk->get_column_by_index(1)->append_datum(v);
        v.set_int16(42);
        chunk->get_column_by_index(2)->append_datum(v);
        v.set_uint8(1);
        chunk->get_column_by_index(3)->append_datum(v);
        v.set_int32(i * 3);
        chunk->get_column_by_index(4)->append_datum(v);
        v.set_int8(i % 5 == 0 ? TOpType::DELETE : TOpType::UPSERT);
        chunk->get_column_by_index(5)->append_datum(v);
    }
    vector<uint32_t> indexes;
    indexes.reserve(n);
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    for (int i = 0; i < n; i++) {
        indexes.emplace_back(i);
    }
    std::random_shuffle(indexes.begin(), indexes.end());
    _mem_table->insert(*chunk, indexes.data(), 0, indexes.size());
    ASSERT_FALSE(_mem_table->finalize().ok());
}

} // namespace starrocks::vectorized
