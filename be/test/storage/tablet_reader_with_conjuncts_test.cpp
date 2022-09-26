// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <memory>

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "exprs/vectorized/binary_predicate.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/literal.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "fs/fs_memory.h"
#include <gtest/gtest.h>
#include "runtime/descriptor_helper.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/olap_common.h"
#include "storage/predicate_parser.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

using namespace starrocks::vectorized;

using VSchema = starrocks::vectorized::Schema;
using VChunk = starrocks::vectorized::Chunk;

class PrimaryKeyTableTest: public testing::Test {
public:
    void SetUp() override {

        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        _state = std::make_shared<RuntimeState>(globals);

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
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
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

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes,
                                          std::shared_ptr<TabletSchema> partial_schema) {
        // create partial rowset
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;

        writer_context.partial_update_tablet_schema = partial_schema;
        writer_context.referenced_column_ids = column_indexes;
        writer_context.tablet_schema = partial_schema.get();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(*partial_schema.get());

        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        EXPECT_TRUE(2 == chunk->num_columns());
        auto& cols = chunk->columns();
        for (size_t i = 0; i < keys.size(); i++) {
            cols[0]->append_datum(vectorized::Datum(keys[i]));
            cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 3)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

    // Build an expression: col < literal
    Expr* build_predicate(PrimitiveType ptype, TExprOpcode::type op, SlotDescriptor* slot) {
        TExprNode expr_node;
        expr_node.opcode = op;
        expr_node.child_type = to_thrift(ptype);
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        Expr* expr = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

        ColumnRef* column_ref = _pool.add(new ColumnRef(slot));
        TExprNode expr_node2;
        expr_node2.child_type = TPrimitiveType::INT;
        expr_node2.node_type = TExprNodeType::INT_LITERAL;
        expr_node2.num_children = 1;
        expr_node2.__isset.opcode = true;
        expr_node2.__isset.child_type = true;
        expr_node2.type = gen_type_desc(TPrimitiveType::INT);
        TIntLiteral lit_value;
        lit_value.__set_value(1);
        expr_node2.__set_int_literal(lit_value);
        expr_node2.__set_use_vectorized(true);
        Expr* col2 = _pool.add(new VectorizedLiteral(expr_node2));
        expr->_children.push_back(column_ref);
        expr->_children.push_back(col2);
        return expr;
    }

    TSlotDescriptor _create_slot_desc(PrimitiveType type, const std::string& col_name, int col_pos) {
        TSlotDescriptorBuilder builder;

        if (type == PrimitiveType::TYPE_VARCHAR || type == PrimitiveType::TYPE_CHAR) {
            return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
        } else {
            return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
        }
    }

    TupleDescriptor* _create_tuple_desc(PrimitiveType ptype) {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(ptype, "pk", 0));
        tuple_builder.add_slot(_create_slot_desc(ptype, "v1", 1));
        tuple_builder.add_slot(_create_slot_desc(ptype, "v2", 2));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

protected:
    using PredicatePtr = std::unique_ptr<vectorized::ColumnPredicate>;

    ObjectPool _pool;
    std::shared_ptr<RuntimeState> _state;
    TabletSharedPtr _tablet;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
};


static ssize_t read_until_eof(const vectorized::ChunkIteratorPtr& iter) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
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

TEST_F(PrimaryKeyTableTest, TestReadWithPK) {
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

    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    auto reader = std::make_shared<TabletReader>(_tablet, Version(0, rowsets.size()), schema);

    auto ptype = PrimitiveType::TYPE_INT;
    auto op = TExprOpcode::EQ;
    TupleDescriptor* tuple_desc = _create_tuple_desc(ptype);
    std::vector<std::string> key_column_names = {"pk"};
    SlotDescriptor* slot = tuple_desc->slots()[0];
    std::vector<ExprContext*> conjunct_ctxs = {_pool.add(new ExprContext(build_predicate(ptype, op, slot)))};

    OlapScanConjunctsManager cm;
    cm.conjunct_ctxs_ptr = &conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_pool;
    cm.key_column_names = &key_column_names;
    cm.runtime_filters = _pool.add(new RuntimeFilterProbeCollector());
    // Parse conjuncts via _conjuncts_manager.
    cm.parse_conjuncts(true, config::max_scan_key_num, true);
    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    cm.get_key_ranges(&key_ranges);

    vectorized::TabletReaderParams params;
    params.is_pipeline = true;
    params.chunk_size = 1024;
    params.reader_type = READER_QUERY;
    params.skip_aggregation = true;
    params.runtime_state = _state.get();
    std::cout<<"key_ranges size:"<<key_ranges.size()<<std::endl;

    PredicateParser parser(_tablet->tablet_schema());
    std::vector<PredicatePtr> predicate_free_pool;
    std::vector<PredicatePtr> preds;
    cm.get_column_predicates(&parser, &preds);
    for (auto& p : preds) {
        if (parser.can_pushdown(p.get())) {
            params.predicates.push_back(p.get());
        }
        predicate_free_pool.emplace_back(std::move(p));
    }

    // TODO: here codes are not used, use key_range is better?
    // Range
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }
        params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                 : TabletReaderParams::RangeStartOperation::GT;
        params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                   : TabletReaderParams::RangeEndOperation::LT;

        params.start_key.push_back(key_range->begin_scan_range);
        params.end_key.push_back(key_range->end_scan_range);
    }

    if (!reader->prepare().ok()) {
        LOG(ERROR) << "reader prepare failed";
    }
    if (!reader->open(params).ok()) {
        LOG(ERROR) << "reader open failed";
    }
    std::shared_ptr<vectorized::ChunkIterator> prj_iter = reader;
    ASSERT_EQ(read_until_eof(prj_iter), 1);

    if (prj_iter) {
        prj_iter->close();
    }
    if (reader) {
        reader.reset();
    }
    predicate_free_pool.clear();
}

} // namespace starrocks::vectorized
