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

#include <map>

#include "gtest/gtest.h"

namespace starrocks {

class PostingListTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Helper to collect postings via for_each_posting
    static std::map<rowid_t, roaring::Roaring> collect_postings(const PostingList& posting_list) {
        std::map<rowid_t, roaring::Roaring> result;
        auto status = posting_list.for_each_posting([&result](rowid_t doc_id, const roaring::Roaring& positions) {
            result[doc_id] = positions;
            return Status::OK();
        });
        EXPECT_TRUE(status.ok());
        return result;
    }
};

TEST_F(PostingListTest, test_empty_posting_list) {
    PostingList posting_list;

    EXPECT_EQ(0, posting_list.get_num_doc_ids());

    auto postings = collect_postings(posting_list);
    EXPECT_TRUE(postings.empty());
}

TEST_F(PostingListTest, test_single_posting) {
    PostingList posting_list;

    rowid_t doc_id = 10;
    rowid_t pos = 5;
    posting_list.add_posting(doc_id, pos);

    EXPECT_EQ(1, posting_list.get_num_doc_ids());

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(1, postings.size());
    EXPECT_TRUE(postings.contains(doc_id));
    EXPECT_EQ(1, postings[doc_id].cardinality());
    EXPECT_TRUE(postings[doc_id].contains(pos));
}

TEST_F(PostingListTest, test_multiple_postings_same_doc_id) {
    PostingList posting_list;

    rowid_t doc_id = 100;
    posting_list.add_posting(doc_id, 1);
    posting_list.add_posting(doc_id, 5);
    posting_list.add_posting(doc_id, 10);

    posting_list.finalize();

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(1, postings.size());
    EXPECT_TRUE(postings.contains(doc_id));
    EXPECT_EQ(3, postings[doc_id].cardinality());
    EXPECT_TRUE(postings[doc_id].contains(1));
    EXPECT_TRUE(postings[doc_id].contains(5));
    EXPECT_TRUE(postings[doc_id].contains(10));
}

TEST_F(PostingListTest, test_multiple_postings_different_doc_ids) {
    PostingList posting_list;

    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.add_posting(3, 30);
    posting_list.add_posting(1, 11);
    posting_list.add_posting(2, 21);

    posting_list.finalize();

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(3, postings.size());
    EXPECT_EQ(3, posting_list.get_num_doc_ids());
    EXPECT_TRUE(postings.contains(1));
    EXPECT_TRUE(postings.contains(2));
    EXPECT_TRUE(postings.contains(3));

    EXPECT_EQ(2, postings[1].cardinality());
    EXPECT_TRUE(postings[1].contains(10));
    EXPECT_TRUE(postings[1].contains(11));

    EXPECT_EQ(2, postings[2].cardinality());
    EXPECT_TRUE(postings[2].contains(20));
    EXPECT_TRUE(postings[2].contains(21));

    EXPECT_EQ(1, postings[3].cardinality());
    EXPECT_TRUE(postings[3].contains(30));
}

TEST_F(PostingListTest, test_move_constructor) {
    PostingList posting_list;
    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.finalize();

    PostingList moved_list(std::move(posting_list));

    auto postings = collect_postings(moved_list);
    EXPECT_EQ(2, postings.size());
    EXPECT_EQ(2, moved_list.get_num_doc_ids());
    EXPECT_TRUE(postings.contains(1));
    EXPECT_TRUE(postings.contains(2));
    EXPECT_TRUE(postings[1].contains(10));
    EXPECT_TRUE(postings[2].contains(20));
}

TEST_F(PostingListTest, test_move_assignment) {
    PostingList posting_list;
    posting_list.add_posting(5, 50);
    posting_list.add_posting(6, 60);
    posting_list.finalize();

    PostingList assigned_list = std::move(posting_list);

    auto postings = collect_postings(assigned_list);
    EXPECT_EQ(2, postings.size());
    EXPECT_EQ(2, assigned_list.get_num_doc_ids());
    EXPECT_TRUE(postings.contains(5));
    EXPECT_TRUE(postings.contains(6));
    EXPECT_TRUE(postings[5].contains(50));
    EXPECT_TRUE(postings[6].contains(60));
}

TEST_F(PostingListTest, test_finalize_empty_list) {
    PostingList posting_list;
    posting_list.finalize();

    EXPECT_EQ(0, posting_list.get_num_doc_ids());

    auto postings = collect_postings(posting_list);
    EXPECT_TRUE(postings.empty());
}

TEST_F(PostingListTest, test_finalize_single_value) {
    PostingList posting_list;
    posting_list.add_posting(100, 200);
    posting_list.finalize();

    EXPECT_EQ(1, posting_list.get_num_doc_ids());

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(1, postings.size());
    EXPECT_TRUE(postings[100].contains(200));
}

TEST_F(PostingListTest, test_large_number_of_postings) {
    PostingList posting_list;

    constexpr int num_docs = 100;
    constexpr int positions_per_doc = 10;

    for (rowid_t doc_id = 0; doc_id < num_docs; doc_id++) {
        for (rowid_t pos = 0; pos < positions_per_doc; pos++) {
            posting_list.add_posting(doc_id, pos);
        }
    }

    posting_list.finalize();

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(num_docs, postings.size());
    EXPECT_EQ(num_docs, posting_list.get_num_doc_ids());

    for (rowid_t doc_id = 0; doc_id < num_docs; doc_id++) {
        EXPECT_TRUE(postings.contains(doc_id));
        EXPECT_EQ(positions_per_doc, postings[doc_id].cardinality());
        for (rowid_t pos = 0; pos < positions_per_doc; pos++) {
            EXPECT_TRUE(postings[doc_id].contains(pos));
        }
    }
}

TEST_F(PostingListTest, test_for_each_posting_error_propagation) {
    PostingList posting_list;
    posting_list.add_posting(1, 10);
    posting_list.add_posting(2, 20);
    posting_list.finalize();

    int call_count = 0;
    auto status = posting_list.for_each_posting([&call_count](rowid_t doc_id, const roaring::Roaring& positions) {
        call_count++;
        if (doc_id == 2) {
            return Status::InternalError("Test error");
        }
        return Status::OK();
    });

    // The callback should have been called and returned error
    EXPECT_TRUE(call_count >= 1);
}

TEST_F(PostingListTest, test_boundary_values) {
    PostingList posting_list;

    rowid_t max_doc_id = std::numeric_limits<rowid_t>::max();
    rowid_t max_pos = std::numeric_limits<rowid_t>::max();

    posting_list.add_posting(0, 0);
    posting_list.add_posting(max_doc_id, max_pos);

    posting_list.finalize();

    auto postings = collect_postings(posting_list);
    EXPECT_EQ(2, postings.size());
    EXPECT_EQ(2, posting_list.get_num_doc_ids());
    EXPECT_TRUE(postings.contains(0));
    EXPECT_TRUE(postings.contains(max_doc_id));
    EXPECT_TRUE(postings[0].contains(0));
    EXPECT_TRUE(postings[max_doc_id].contains(max_pos));
}

} // namespace starrocks
