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

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "fs/fs_memory.h"
#include "storage/rowset/indexed_column_reader.h"
#include "storage/rowset/indexed_column_writer.h"

namespace starrocks {

class IndexedColumnTest : public testing::Test {
protected:
    void create_column(const std::vector<std::vector<std::string>>& pages) {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file("/indexed_column"));
        IndexedColumnWriterOptions options;
        options.write_value_index = true;
        options.write_ordinal_index = true;
        options.encoding = PREFIX_ENCODING;
        IndexedColumnWriter writer(options, get_type_info(TYPE_VARCHAR), wfile.get());
        ASSERT_OK(writer.init());
        for (const auto& page : pages) {
            for (const auto& value : page) {
                Slice slice(value);
                ASSERT_OK(writer.add(&slice));
                _values.push_back(value);
            }
            // Make the page gaps explicit without depending on the default page size.
            ASSERT_OK(writer._finish_current_data_page());
        }
        ASSERT_OK(writer.finish(&_meta));
        ASSERT_OK(wfile->close());

        ASSIGN_OR_ABORT(_rfile, _fs->new_random_access_file("/indexed_column"));
        IndexReadOptions read_options;
        read_options.read_file = _rfile.get();
        read_options.stats = &_stats;
        read_options.use_page_cache = false;
        _reader = std::make_unique<IndexedColumnReader>(_meta);
        ASSERT_OK(_reader->load(read_options));
        ASSERT_OK(_reader->new_iterator(read_options, &_iter));
    }

    void check_seek(const std::string& key) {
        SCOPED_TRACE(key);
        auto expected = std::lower_bound(_values.begin(), _values.end(), key);
        Slice slice(key);
        bool exact_match = true;
        Status status = _iter->seek_at_or_after(&slice, &exact_match);
        if (expected == _values.end()) {
            ASSERT_TRUE(status.is_not_found()) << status.to_string();
            return;
        }
        ASSERT_OK(status);
        ASSERT_EQ(*expected == key, exact_match);
        ASSERT_EQ(expected - _values.begin(), _iter->get_current_ordinal());

        // Reading the suffix also checks that a cross-page seek updates the page
        // position and the index iterator used by subsequent next_batch calls.
        auto column = BinaryColumn::create();
        size_t count = _values.size();
        ASSERT_OK(_iter->next_batch(&count, column.get()));
        ASSERT_EQ(_values.end() - expected, count);
        ASSERT_EQ(count, column->size());
        for (size_t i = 0; i < count; ++i) {
            ASSERT_EQ(expected[i], column->get_slice(i).to_string());
        }
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    OlapReaderStatistics _stats;
    IndexedColumnMetaPB _meta;
    std::vector<std::string> _values;
    std::unique_ptr<RandomAccessFile> _rfile;
    std::unique_ptr<IndexedColumnReader> _reader;
    std::unique_ptr<IndexedColumnIterator> _iter;
};

TEST_F(IndexedColumnTest, SeekAcrossValueIndexPages) {
    ASSERT_NO_FATAL_FAILURE(create_column({{"b", "d"}, {"f", "h"}, {"j", "l"}}));
    ASSERT_FALSE(_meta.value_index_meta().is_root_data_page());
    // e and i lie in different page gaps. Repeated forward/backward seeks cover
    // page reuse, exact boundary hits, and recovery after a genuine end-of-index miss.
    for (const auto* key : {"e", "i", "c", "a", "f", "d", "j", "l", "m", "b", "h", "e"}) {
        ASSERT_NO_FATAL_FAILURE(check_seek(key));
    }
}

TEST_F(IndexedColumnTest, SeekInSingleValueIndexPage) {
    ASSERT_NO_FATAL_FAILURE(create_column({{"b", "d"}}));
    ASSERT_TRUE(_meta.value_index_meta().is_root_data_page());
    for (const auto* key : {"a", "b", "c", "d", "e", "b"}) {
        ASSERT_NO_FATAL_FAILURE(check_seek(key));
    }
}

TEST_F(IndexedColumnTest, SeekInEmptyValueIndex) {
    ASSERT_NO_FATAL_FAILURE(create_column({}));
    ASSERT_NO_FATAL_FAILURE(check_seek("a"));
}

} // namespace starrocks
