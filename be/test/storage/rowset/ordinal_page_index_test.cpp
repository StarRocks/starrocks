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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/ordinal_page_index_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/ordinal_page_index.h"

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <string>

#include "common/logging.h"
#include "fs/fs_memory.h"
#include "runtime/mem_tracker.h"
#include "storage/page_cache.h"
#include "testutil/assert.h"

namespace starrocks {

class OrdinalPageIndexTest : public testing::Test {
public:
    const std::string kTestDir = "/ordinal_page_index_test";

    void SetUp() override {
        _mem_tracker = std::make_unique<MemTracker>();
        StoragePageCache::create_global_cache(_mem_tracker.get(), 1000000000);
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    void TearDown() override { StoragePageCache::release_global_cache(); }

protected:
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
};

TEST_F(OrdinalPageIndexTest, normal) {
    std::string filename = kTestDir + "/normal.idx";

    OrdinalIndexWriter builder;
    // generate ordinal index for 16K data pages,
    // each data page is 16KB in size and contains 4096 values,
    // ordinal starts at 1 instead of 0
    for (uint64_t i = 0; i < 16 * 1024; ++i) {
        builder.append_entry(1 + 4096 * i, {16 * 1024 * i, 16 * 1024});
    }
    ColumnIndexMetaPB index_meta;
    {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));

        ASSERT_TRUE(builder.finish(wfile.get(), &index_meta).ok());
        ASSERT_EQ(ORDINAL_INDEX, index_meta.type());
        ASSERT_FALSE(index_meta.ordinal_index().root_page().is_root_data_page());
        ASSERT_OK(wfile->close());
        LOG(INFO) << "index page size=" << index_meta.ordinal_index().root_page().root_page().size();
    }

    OrdinalIndexReader index;
    ASSIGN_OR_ABORT(auto r,
                    index.load(_fs.get(), filename, index_meta.ordinal_index(), 16 * 1024 * 4096 + 1, true, false));
    ASSERT_TRUE(r);
    ASSERT_EQ(16 * 1024, index.num_data_pages());
    ASSERT_EQ(1, index.get_first_ordinal(0));
    ASSERT_EQ(4096, index.get_last_ordinal(0));
    ASSERT_EQ((16 * 1024 - 1) * 4096 + 1, index.get_first_ordinal(16 * 1024 - 1));
    ASSERT_EQ(16 * 1024 * 4096, index.get_last_ordinal(16 * 1024 - 1));

    {
        auto iter = index.seek_at_or_before(1);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(1, iter.first_ordinal());
        ASSERT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4095);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(1, iter.first_ordinal());
        ASSERT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4098);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4097, iter.first_ordinal());
        ASSERT_EQ(PagePointer(1 * 16 * 1024, 16 * 1024), iter.page());

        iter.next();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4097 + 4096, iter.first_ordinal());
        ASSERT_EQ(PagePointer(2 * 16 * 1024, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(0);
        ASSERT_FALSE(iter.valid());
    }
}

TEST_F(OrdinalPageIndexTest, one_data_page) {
    // index one data page with 1024 values
    int num_values = 1024;
    PagePointer data_page_pointer(0, 4096);

    OrdinalIndexWriter builder;
    builder.append_entry(0, data_page_pointer); // add only one entry
    ColumnIndexMetaPB index_meta;
    {
        // in this case, no index page is written, thus file could be null
        ASSERT_TRUE(builder.finish(nullptr, &index_meta).ok());
        ASSERT_EQ(ORDINAL_INDEX, index_meta.type());
        ASSERT_TRUE(index_meta.ordinal_index().root_page().is_root_data_page());
        PagePointer root_page_pointer(index_meta.ordinal_index().root_page().root_page());
        ASSERT_EQ(data_page_pointer, root_page_pointer);
    }

    OrdinalIndexReader index;
    ASSIGN_OR_ABORT(auto r, index.load(_fs.get(), "", index_meta.ordinal_index(), num_values, true, false));
    ASSERT_TRUE(r);
    ASSERT_EQ(1, index.num_data_pages());
    ASSERT_EQ(0, index.get_first_ordinal(0));
    ASSERT_EQ(num_values - 1, index.get_last_ordinal(0));

    {
        auto iter = index.seek_at_or_before(0);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.first_ordinal());
        ASSERT_EQ(num_values - 1, iter.last_ordinal());
        ASSERT_EQ(data_page_pointer, iter.page());
    }
    {
        auto iter = index.seek_at_or_before(num_values - 1);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.first_ordinal());
        ASSERT_EQ(data_page_pointer, iter.page());
    }
    {
        auto iter = index.seek_at_or_before(num_values);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.first_ordinal());
        ASSERT_EQ(data_page_pointer, iter.page());
    }
}

} // namespace starrocks
