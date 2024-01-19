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

#include "tablet_updates_test.h"

namespace starrocks {

void TabletUpdatesTest::test_link_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet2 = create_tablet2(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    _tablet2->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    _tablet2->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<ChunkChanger>(_tablet2->tablet_schema());
    ASSERT_TRUE(_tablet2->updates()->link_from(_tablet.get(), 4, chunk_changer.get(), _tablet->tablet_schema()).ok());

    ASSERT_EQ(N, read_tablet(_tablet2, 4));
}

TEST_F(TabletUpdatesTest, link_from) {
    test_link_from(false);
}

TEST_F(TabletUpdatesTest, link_from_with_persistent_index) {
    test_link_from(true);
}

void TabletUpdatesTest::test_schema_change_optimiazation_adding_generated_column(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto base_tablet = create_tablet(rand(), rand());
    base_tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& new_tablet = create_tablet_to_add_generated_column(rand(), rand());
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(base_tablet->rowset_commit(2, create_rowset(base_tablet, keys)).ok());
    ASSERT_TRUE(base_tablet->rowset_commit(3, create_rowset(base_tablet, keys)).ok());
    ASSERT_TRUE(base_tablet->rowset_commit(4, create_rowset(base_tablet, keys)).ok());

    new_tablet->set_tablet_state(TABLET_NOTREADY);

    TAlterTabletReqV2 request;
    TAlterTabletMaterializedColumnReq mc_request;

    std::vector<TExprNode> nodes;

    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.type = gen_type_desc(TPrimitiveType::BIGINT);
    node.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node.__set_slot_ref(t_slot_ref);
    node.is_nullable = true;
    nodes.emplace_back(node);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::map<int32_t, starrocks::TExpr> m_expr;
    m_expr.insert({3, t_expr});

    mc_request.__set_query_globals(TQueryGlobals());
    mc_request.__set_query_options(TQueryOptions());
    mc_request.__set_mc_exprs(m_expr);

    request.__set_base_schema_hash(base_tablet->schema_hash());
    request.__set_new_schema_hash(new_tablet->schema_hash());
    request.__set_base_tablet_id(base_tablet->tablet_id());
    request.__set_new_tablet_id(new_tablet->tablet_id());
    request.__set_alter_version(base_tablet->max_version().second);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    request.__set_materialized_column_req(mc_request);
    request.__set_txn_id(99);
    request.__set_job_id(999);

    std::string alter_msg_header = strings::Substitute("[Alter Job:$0, tablet:$1]: ", 999, base_tablet->tablet_id());
    SchemaChangeHandler handler;
    handler.set_alter_msg_header(alter_msg_header);
    auto res = handler.process_alter_tablet_v2(request);
    ASSERT_TRUE(res.ok()) << res.to_string();
}

TEST_F(TabletUpdatesTest, test_schema_change_optimiazation_adding_generated_column) {
    test_schema_change_optimiazation_adding_generated_column(false);
}

TEST_F(TabletUpdatesTest, test_schema_change_optimiazation_adding_generated_column_with_persistent_index) {
    test_schema_change_optimiazation_adding_generated_column(true);
}

void TabletUpdatesTest::test_convert_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->unsafe_tablet_schema_ref().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->unsafe_tablet_schema_ref().column(i);
        int32_t column_index = _tablet->field_index_with_max_version(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(chunk_changer->prepare().ok());
    ASSERT_TRUE(tablet_to_schema_change->updates()
                        ->convert_from(_tablet, 4, chunk_changer.get(), _tablet->tablet_schema())
                        .ok());

    ASSERT_EQ(N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, keys));
}

void TabletUpdatesTest::test_convert_from_with_pending(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    int N = 100;
    std::vector<int64_t> keys2;   // [0, 100)
    std::vector<int64_t> keys3;   // [50, 150)
    std::vector<int64_t> keys4;   // [100, 200)
    std::vector<int64_t> allkeys; // [0, 200)
    for (int i = 0; i < N; i++) {
        keys2.push_back(i);
        keys3.push_back(N / 2 + i);
        keys4.push_back(N + i);
        allkeys.push_back(i * 2);
        allkeys.push_back(i * 2 + 1);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys2)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->unsafe_tablet_schema_ref().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->unsafe_tablet_schema_ref().column(i);
        int32_t column_index = _tablet->field_index_with_max_version(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(chunk_changer->prepare().ok());
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(3, create_rowset(tablet_to_schema_change, keys3)).ok());
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(4, create_rowset(tablet_to_schema_change, keys4)).ok());

    ASSERT_TRUE(tablet_to_schema_change->updates()
                        ->convert_from(_tablet, 2, chunk_changer.get(), _tablet->tablet_schema())
                        .ok());

    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys3)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys4)).ok());

    ASSERT_EQ(2 * N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, allkeys));
}

void TabletUpdatesTest::test_convert_from_with_mutiple_segment(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    std::vector<std::vector<int64_t>> keys_by_segment;
    keys_by_segment.resize(2);
    for (int i = 100; i < 200; i++) {
        keys_by_segment[0].emplace_back(i);
    }
    for (int i = 0; i < 100; i++) {
        keys_by_segment[1].emplace_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset_with_mutiple_segments(_tablet, keys_by_segment)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->unsafe_tablet_schema_ref().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->unsafe_tablet_schema_ref().column(i);
        int32_t column_index = _tablet->field_index_with_max_version(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(chunk_changer->prepare().ok());
    std::vector<int64_t> ori_keys;
    for (int i = 100; i < 200; i++) {
        ori_keys.emplace_back(i);
    }
    for (int i = 0; i < 100; i++) {
        ori_keys.emplace_back(i);
    }
    ASSERT_EQ(200, read_tablet_and_compare(_tablet, 2, ori_keys));

    ASSERT_TRUE(tablet_to_schema_change->updates()
                        ->convert_from(_tablet, 2, chunk_changer.get(), _tablet->tablet_schema())
                        .ok());

    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) {
        keys.emplace_back(i);
    }
    ASSERT_EQ(200, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 2, keys));
}

TEST_F(TabletUpdatesTest, convert_from) {
    test_convert_from(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_persistent_index) {
    test_convert_from(true);
}

TEST_F(TabletUpdatesTest, convert_from_with_pending) {
    test_convert_from_with_pending(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_pending_and_persistent_index) {
    test_convert_from_with_pending(true);
}

TEST_F(TabletUpdatesTest, convert_from_with_mutiple_segment) {
    test_convert_from_with_mutiple_segment(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_mutiple_segment_with_persistent_index) {
    test_convert_from_with_mutiple_segment(true);
}

void TabletUpdatesTest::test_reorder_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_with_sort_key1 = create_tablet_with_sort_key(rand(), rand(), {1});
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset_schema_change_sort_key(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset_schema_change_sort_key(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset_schema_change_sort_key(_tablet, keys)).ok());

    tablet_with_sort_key1->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<ChunkChanger>(tablet_with_sort_key1->tablet_schema());
    for (int i = 0; i < tablet_with_sort_key1->unsafe_tablet_schema_ref().num_columns(); ++i) {
        const auto& new_column = tablet_with_sort_key1->unsafe_tablet_schema_ref().column(i);
        int32_t column_index = _tablet->field_index_with_max_version(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(chunk_changer->prepare().ok());

    ASSERT_TRUE(tablet_with_sort_key1->updates()
                        ->reorder_from(_tablet, 4, chunk_changer.get(), _tablet->tablet_schema())
                        .ok());

    ASSERT_EQ(N, read_tablet_and_compare_schema_changed_sort_key1(tablet_with_sort_key1, 4, keys));

    const auto& tablet_with_sort_key2 = create_tablet_with_sort_key(rand(), rand(), {2});
    tablet_with_sort_key2->set_tablet_state(TABLET_NOTREADY);
    chunk_changer = std::make_unique<ChunkChanger>(tablet_with_sort_key2->tablet_schema());
    for (int i = 0; i < tablet_with_sort_key2->unsafe_tablet_schema_ref().num_columns(); ++i) {
        const auto& new_column = tablet_with_sort_key2->unsafe_tablet_schema_ref().column(i);
        int32_t column_index = _tablet->field_index_with_max_version(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(chunk_changer->prepare().ok());
    ASSERT_TRUE(tablet_with_sort_key2->updates()
                        ->reorder_from(tablet_with_sort_key1, 4, chunk_changer.get(),
                                       tablet_with_sort_key1->tablet_schema())
                        .ok());
    ASSERT_EQ(N, read_tablet_and_compare_schema_changed_sort_key2(tablet_with_sort_key2, 4, keys));
}

TEST_F(TabletUpdatesTest, reorder_from) {
    test_reorder_from(false);
}

TEST_F(TabletUpdatesTest, reorder_from_with_persistent_index) {
    test_reorder_from(true);
}

TEST_F(TabletUpdatesTest, test_schema_change_using_base_tablet_max_version) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet2 = create_tablet(rand(), rand());
    _tablet2->set_tablet_state(TABLET_NOTREADY);
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // schema change began, double write, but alter request not reach tablet yet
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet2->rowset_commit(3, create_rowset(_tablet2, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet2->rowset_commit(4, create_rowset(_tablet2, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // alter request reach tablet, using base tablet max version:4 should work
    auto chunk_changer = std::make_unique<ChunkChanger>(_tablet2->tablet_schema());
    ASSERT_TRUE(_tablet2->updates()->link_from(_tablet.get(), 4, chunk_changer.get(), _tablet->tablet_schema()).ok());

    ASSERT_EQ(N, read_tablet(_tablet2, 4));
}

} // namespace starrocks
