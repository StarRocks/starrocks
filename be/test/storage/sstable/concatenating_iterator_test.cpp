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

#include "storage/sstable/concatenating_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/iterator.h"

namespace starrocks::sstable {

// A simple in-memory iterator for testing
class VectorIterator : public Iterator {
public:
    VectorIterator(const std::vector<std::pair<std::string, std::string>>& data) : _data(data), _index(-1) {}

    bool Valid() const override { return _index >= 0 && _index < static_cast<int>(_data.size()); }

    void SeekToFirst() override { _index = _data.empty() ? -1 : 0; }

    void SeekToLast() override { _index = _data.empty() ? -1 : static_cast<int>(_data.size()) - 1; }

    void Seek(const Slice& target) override {
        for (size_t i = 0; i < _data.size(); i++) {
            if (_data[i].first >= target.to_string()) {
                _index = i;
                return;
            }
        }
        _index = -1;
    }

    void Next() override {
        assert(Valid());
        _index++;
        if (_index >= static_cast<int>(_data.size())) {
            _index = -1;
        }
    }

    void Prev() override {
        assert(Valid());
        _index--;
    }

    Slice key() const override {
        assert(Valid());
        return Slice(_data[_index].first);
    }

    Slice value() const override {
        assert(Valid());
        return Slice(_data[_index].second);
    }

    Status status() const override { return Status::OK(); }

private:
    std::vector<std::pair<std::string, std::string>> _data;
    int _index;
};

class ConcatenatingIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Helper function to collect all keys from an iterator
    std::vector<std::string> CollectKeys(Iterator* iter) {
        std::vector<std::string> keys;
        iter->SeekToFirst();
        while (iter->Valid()) {
            keys.push_back(iter->key().to_string());
            iter->Next();
        }
        return keys;
    }

    // Helper function to collect all keys in reverse order
    std::vector<std::string> CollectKeysReverse(Iterator* iter) {
        std::vector<std::string> keys;
        iter->SeekToLast();
        while (iter->Valid()) {
            keys.push_back(iter->key().to_string());
            iter->Prev();
        }
        return keys;
    }
};

TEST_F(ConcatenatingIteratorTest, EmptyIterators) {
    Iterator* children[0] = {};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 0));

    ASSERT_FALSE(iter->Valid());
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
}

TEST_F(ConcatenatingIteratorTest, SingleIterator) {
    std::vector<std::pair<std::string, std::string>> data = {{"a", "1"}, {"c", "2"}, {"e", "3"}};

    Iterator* children[1] = {new VectorIterator(data)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 1));
    iter->SeekToFirst();
    EXPECT_EQ(iter->max_rss_rowid(), 0);
    EXPECT_EQ(iter->shared_rssid(), 0);
    EXPECT_EQ(iter->shared_version(), 0);

    auto keys = CollectKeys(iter.get());
    ASSERT_EQ(3, keys.size());
    EXPECT_EQ("a", keys[0]);
    EXPECT_EQ("c", keys[1]);
    EXPECT_EQ("e", keys[2]);
}

TEST_F(ConcatenatingIteratorTest, MultipleIteratorsSeekToFirst) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}, {"c", "2"}, {"e", "3"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"g", "4"}, {"i", "5"}, {"k", "6"}};
    std::vector<std::pair<std::string, std::string>> data3 = {{"m", "7"}, {"o", "8"}, {"q", "9"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    auto keys = CollectKeys(iter.get());
    ASSERT_EQ(9, keys.size());
    EXPECT_EQ("a", keys[0]);
    EXPECT_EQ("c", keys[1]);
    EXPECT_EQ("e", keys[2]);
    EXPECT_EQ("g", keys[3]);
    EXPECT_EQ("i", keys[4]);
    EXPECT_EQ("k", keys[5]);
    EXPECT_EQ("m", keys[6]);
    EXPECT_EQ("o", keys[7]);
    EXPECT_EQ("q", keys[8]);
}

TEST_F(ConcatenatingIteratorTest, MultipleIteratorsSeekToLast) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}, {"c", "2"}, {"e", "3"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"g", "4"}, {"i", "5"}, {"k", "6"}};
    std::vector<std::pair<std::string, std::string>> data3 = {{"m", "7"}, {"o", "8"}, {"q", "9"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    auto keys = CollectKeysReverse(iter.get());
    ASSERT_EQ(9, keys.size());
    EXPECT_EQ("q", keys[0]);
    EXPECT_EQ("o", keys[1]);
    EXPECT_EQ("m", keys[2]);
    EXPECT_EQ("k", keys[3]);
    EXPECT_EQ("i", keys[4]);
    EXPECT_EQ("g", keys[5]);
    EXPECT_EQ("e", keys[6]);
    EXPECT_EQ("c", keys[7]);
    EXPECT_EQ("a", keys[8]);
}

TEST_F(ConcatenatingIteratorTest, SeekToTarget) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}, {"c", "2"}, {"e", "3"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"g", "4"}, {"i", "5"}, {"k", "6"}};
    std::vector<std::pair<std::string, std::string>> data3 = {{"m", "7"}, {"o", "8"}, {"q", "9"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    // Seek to a key in the first iterator
    iter->Seek(Slice("b"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c", iter->key().to_string());

    // Seek to a key in the second iterator
    iter->Seek(Slice("h"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("i", iter->key().to_string());

    // Seek to a key in the third iterator
    iter->Seek(Slice("n"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("o", iter->key().to_string());

    // Seek to exact key
    iter->Seek(Slice("k"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("k", iter->key().to_string());

    // Seek beyond all keys
    iter->Seek(Slice("z"));
    ASSERT_FALSE(iter->Valid());
}

TEST_F(ConcatenatingIteratorTest, WithEmptyChildren) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}, {"c", "2"}};
    std::vector<std::pair<std::string, std::string>> data2; // empty
    std::vector<std::pair<std::string, std::string>> data3 = {{"m", "7"}, {"o", "8"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    auto keys = CollectKeys(iter.get());
    ASSERT_EQ(4, keys.size());
    EXPECT_EQ("a", keys[0]);
    EXPECT_EQ("c", keys[1]);
    EXPECT_EQ("m", keys[2]);
    EXPECT_EQ("o", keys[3]);
}

TEST_F(ConcatenatingIteratorTest, NextAcrossBoundaries) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"b", "2"}};
    std::vector<std::pair<std::string, std::string>> data3 = {{"c", "3"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("a", iter->key().to_string());
    EXPECT_EQ("1", iter->value().to_string());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b", iter->key().to_string());
    EXPECT_EQ("2", iter->value().to_string());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c", iter->key().to_string());
    EXPECT_EQ("3", iter->value().to_string());

    iter->Next();
    ASSERT_FALSE(iter->Valid());
}

TEST_F(ConcatenatingIteratorTest, PrevAcrossBoundaries) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"b", "2"}};
    std::vector<std::pair<std::string, std::string>> data3 = {{"c", "3"}};

    Iterator* children[3] = {new VectorIterator(data1), new VectorIterator(data2), new VectorIterator(data3)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 3));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("c", iter->key().to_string());

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("b", iter->key().to_string());

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ("a", iter->key().to_string());

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
}

TEST_F(ConcatenatingIteratorTest, ForwardAndBackwardMixed) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}, {"b", "2"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"c", "3"}, {"d", "4"}};

    Iterator* children[2] = {new VectorIterator(data1), new VectorIterator(data2)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 2));

    iter->SeekToFirst();
    EXPECT_EQ("a", iter->key().to_string());

    iter->Next();
    EXPECT_EQ("b", iter->key().to_string());

    iter->Next();
    EXPECT_EQ("c", iter->key().to_string());

    iter->Prev();
    EXPECT_EQ("b", iter->key().to_string());

    iter->Prev();
    EXPECT_EQ("a", iter->key().to_string());

    iter->Next();
    iter->Next();
    iter->Next();
    EXPECT_EQ("d", iter->key().to_string());
}

TEST_F(ConcatenatingIteratorTest, StatusCheck) {
    std::vector<std::pair<std::string, std::string>> data1 = {{"a", "1"}};
    std::vector<std::pair<std::string, std::string>> data2 = {{"b", "2"}};

    Iterator* children[2] = {new VectorIterator(data1), new VectorIterator(data2)};
    std::unique_ptr<Iterator> iter(NewConcatenatingIterator(children, 2));

    ASSERT_TRUE(iter->status().ok());
    iter->SeekToFirst();
    ASSERT_TRUE(iter->status().ok());
}

} // namespace starrocks::sstable
