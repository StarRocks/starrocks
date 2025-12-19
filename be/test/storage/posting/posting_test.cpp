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

#include "storage/posting/posting.h"

#include "gtest/gtest.h"

namespace starrocks {

class PostingListTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PostingListTest, test_empty_posting_list) {
    PostingList posting_list;

    EXPECT_EQ(0, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_TRUE(all_doc_ids.isEmpty());

    roaring::Roaring positions = posting_list.get_positions(0);
    EXPECT_TRUE(positions.isEmpty());
}

TEST_F(PostingListTest, test_single_posting) {
    PostingList posting_list;

    rowid_t doc_id = 10;
    rowid_t pos = 5;
    posting_list.add_posting(doc_id, pos);

    EXPECT_EQ(1, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_EQ(1, all_doc_ids.cardinality());
    EXPECT_TRUE(all_doc_ids.contains(doc_id));

    roaring::Roaring positions = posting_list.get_positions(doc_id);
    EXPECT_EQ(1, positions.cardinality());
    EXPECT_TRUE(positions.contains(pos));

    roaring::Roaring no_positions = posting_list.get_positions(999);
    EXPECT_TRUE(no_positions.isEmpty());
}

TEST_F(PostingListTest, test_multiple_postings_same_doc_id) {
    PostingList posting_list;

    rowid_t doc_id = 100;
    posting_list.add_posting(doc_id, 1);
    posting_list.add_posting(doc_id, 5);
    posting_list.add_posting(doc_id, 10);

    posting_list.finalize();

    EXPECT_EQ(1, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_EQ(1, all_doc_ids.cardinality());
    EXPECT_TRUE(all_doc_ids.contains(doc_id));

    roaring::Roaring positions = posting_list.get_positions(doc_id);
    EXPECT_EQ(3, positions.cardinality());
    EXPECT_TRUE(positions.contains(1));
    EXPECT_TRUE(positions.contains(5));
    EXPECT_TRUE(positions.contains(10));
}

TEST_F(PostingListTest, test_multiple_postings_different_doc_ids) {
    PostingList posting_list;

    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.add_posting(3, 30);
    posting_list.add_posting(1, 11);
    posting_list.add_posting(2, 21);

    posting_list.finalize();

    EXPECT_EQ(3, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_EQ(3, all_doc_ids.cardinality());
    EXPECT_TRUE(all_doc_ids.contains(1));
    EXPECT_TRUE(all_doc_ids.contains(2));
    EXPECT_TRUE(all_doc_ids.contains(3));

    roaring::Roaring positions_1 = posting_list.get_positions(1);
    EXPECT_EQ(2, positions_1.cardinality());
    EXPECT_TRUE(positions_1.contains(10));
    EXPECT_TRUE(positions_1.contains(11));

    roaring::Roaring positions_2 = posting_list.get_positions(2);
    EXPECT_EQ(2, positions_2.cardinality());
    EXPECT_TRUE(positions_2.contains(20));
    EXPECT_TRUE(positions_2.contains(21));

    roaring::Roaring positions_3 = posting_list.get_positions(3);
    EXPECT_EQ(1, positions_3.cardinality());
    EXPECT_TRUE(positions_3.contains(30));
}

TEST_F(PostingListTest, test_move_constructor) {
    PostingList posting_list;
    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.finalize();

    PostingList moved_list(std::move(posting_list));

    EXPECT_EQ(2, moved_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = moved_list.get_all_doc_ids();
    EXPECT_TRUE(all_doc_ids.contains(1));
    EXPECT_TRUE(all_doc_ids.contains(2));

    roaring::Roaring positions_1 = moved_list.get_positions(1);
    EXPECT_TRUE(positions_1.contains(10));

    roaring::Roaring positions_2 = moved_list.get_positions(2);
    EXPECT_TRUE(positions_2.contains(20));
}

TEST_F(PostingListTest, test_move_assignment) {
    PostingList posting_list;
    posting_list.add_posting(5, 50);
    posting_list.add_posting(6, 60);
    posting_list.finalize();

    PostingList assigned_list;
    assigned_list = std::move(posting_list);

    EXPECT_EQ(2, assigned_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = assigned_list.get_all_doc_ids();
    EXPECT_TRUE(all_doc_ids.contains(5));
    EXPECT_TRUE(all_doc_ids.contains(6));

    roaring::Roaring positions_5 = assigned_list.get_positions(5);
    EXPECT_TRUE(positions_5.contains(50));

    roaring::Roaring positions_6 = assigned_list.get_positions(6);
    EXPECT_TRUE(positions_6.contains(60));
}

TEST_F(PostingListTest, test_finalize_empty_list) {
    PostingList posting_list;
    posting_list.finalize();
    EXPECT_EQ(0, posting_list.get_num_doc_ids());
}

TEST_F(PostingListTest, test_finalize_single_value) {
    PostingList posting_list;
    posting_list.add_posting(100, 200);
    posting_list.finalize();

    EXPECT_EQ(1, posting_list.get_num_doc_ids());
    roaring::Roaring positions = posting_list.get_positions(100);
    EXPECT_TRUE(positions.contains(200));
}

TEST_F(PostingListTest, test_large_number_of_postings) {
    PostingList posting_list;

    const int num_docs = 100;
    const int positions_per_doc = 10;

    for (rowid_t doc_id = 0; doc_id < num_docs; doc_id++) {
        for (rowid_t pos = 0; pos < positions_per_doc; pos++) {
            posting_list.add_posting(doc_id, pos);
        }
    }

    posting_list.finalize();

    EXPECT_EQ(num_docs, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_EQ(num_docs, all_doc_ids.cardinality());

    for (rowid_t doc_id = 0; doc_id < num_docs; doc_id++) {
        roaring::Roaring positions = posting_list.get_positions(doc_id);
        EXPECT_EQ(positions_per_doc, positions.cardinality());
        for (rowid_t pos = 0; pos < positions_per_doc; pos++) {
            EXPECT_TRUE(positions.contains(pos));
        }
    }
}

TEST_F(PostingListTest, test_get_positions_nonexistent_doc_id) {
    PostingList posting_list;
    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.finalize();

    roaring::Roaring positions = posting_list.get_positions(999);
    EXPECT_TRUE(positions.isEmpty());
}

TEST_F(PostingListTest, test_boundary_values) {
    PostingList posting_list;

    rowid_t max_doc_id = std::numeric_limits<rowid_t>::max();
    rowid_t max_pos = std::numeric_limits<rowid_t>::max();

    posting_list.add_posting(0, 0);
    posting_list.add_posting(max_doc_id, max_pos);

    posting_list.finalize();

    EXPECT_EQ(2, posting_list.get_num_doc_ids());

    roaring::Roaring all_doc_ids = posting_list.get_all_doc_ids();
    EXPECT_TRUE(all_doc_ids.contains(0));
    EXPECT_TRUE(all_doc_ids.contains(max_doc_id));

    roaring::Roaring positions_0 = posting_list.get_positions(0);
    EXPECT_TRUE(positions_0.contains(0));

    roaring::Roaring positions_max = posting_list.get_positions(max_doc_id);
    EXPECT_TRUE(positions_max.contains(max_pos));
}

} // namespace starrocks
