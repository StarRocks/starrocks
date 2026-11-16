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

#include "column/binary_column.h"
#include "fs/fs_memory.h"
#include "io/seekable_input_stream.h"
#include "storage/rowset/indexed_column_reader.h"
#include "storage/rowset/indexed_column_writer.h"
#include "testutil/assert.h"

namespace starrocks {

class FaultInjectionStream : public io::SeekableInputStreamWrapper {
public:
    explicit FaultInjectionStream(std::shared_ptr<io::SeekableInputStream> stream)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(std::move(stream)) {}

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (fail_read) {
            fail_read = false;
            return Status::IOError("injected read failure");
        }
        return _stream->read_at_fully(offset, out, count);
    }

    bool fail_read = false;

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
};

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

TEST_F(IndexedColumnTest, SeekAcrossValueIndexPagesReadFailureRecovery) {
    ASSERT_NO_FATAL_FAILURE(create_column({{"ab", "adh"}, {"agh", "ahh"}, {"aj", "al"}}));
    auto fault_stream = std::make_shared<FaultInjectionStream>(_rfile->stream());
    auto fault_file = std::make_unique<RandomAccessFile>(fault_stream, "/indexed_column");
    IndexReadOptions read_options;
    read_options.read_file = fault_file.get();
    read_options.stats = &_stats;
    read_options.use_page_cache = false;
    std::unique_ptr<IndexedColumnIterator> iter;
    ASSERT_OK(_reader->new_iterator(read_options, &iter));

    // Warm up page 0 [ab, adh].
    Slice key_ab("ab");
    bool exact_match = false;
    ASSERT_OK(iter->seek_at_or_after(&key_ab, &exact_match));
    ASSERT_TRUE(exact_match);
    ASSERT_EQ(0, iter->get_current_ordinal());

    // Inject a failure when loading the next page [agh, ahh] across the gap at "af".
    fault_stream->fail_read = true;
    Slice key_af("af");
    Status st = iter->seek_at_or_after(&key_af, &exact_match);
    ASSERT_TRUE(st.is_io_error()) << st.to_string();

    // Subsequent seek to ordinal 1 ("adh" in page 0) must not reuse a desynchronized page.
    ASSERT_OK(iter->seek_to_ordinal(1));
    ASSERT_EQ(1, iter->get_current_ordinal());

    // Reading 3 values must yield "adh", "agh", "ahh" rather than skipping page 1 to "aj", "al".
    auto column = BinaryColumn::create();
    size_t count = 3;
    ASSERT_OK(iter->next_batch(&count, column.get()));
    ASSERT_EQ(3, count);
    EXPECT_EQ("adh", column->get_slice(0).to_string());
    EXPECT_EQ("agh", column->get_slice(1).to_string());
    EXPECT_EQ("ahh", column->get_slice(2).to_string());
}

TEST_F(IndexedColumnTest, SeekEndOfIndexMissRecovery) {
    ASSERT_NO_FATAL_FAILURE(create_column({{"ab", "adh"}, {"agh", "ahh"}}));
    Slice key_z("z");
    bool exact_match = false;
    Status st = _iter->seek_at_or_after(&key_z, &exact_match);
    ASSERT_TRUE(st.is_not_found()) << st.to_string();

    // After NotFound at the end of the index, seek_to_ordinal must reposition cleanly.
    ASSERT_OK(_iter->seek_to_ordinal(0));
    ASSERT_EQ(0, _iter->get_current_ordinal());
    auto column = BinaryColumn::create();
    size_t count = 1;
    ASSERT_OK(_iter->next_batch(&count, column.get()));
    ASSERT_EQ(1, count);
    EXPECT_EQ("ab", column->get_slice(0).to_string());
}

class IndexedColumnEndMissTest : public IndexedColumnTest, public testing::WithParamInterface<bool> {};

TEST_P(IndexedColumnEndMissTest, RepeatedEndMissesReusePage) {
    if (GetParam()) {
        ASSERT_NO_FATAL_FAILURE(create_column({{"b", "d"}, {"f", "h"}}));
    } else {
        ASSERT_NO_FATAL_FAILURE(create_column({{"b", "d", "f", "h"}}));
    }

    Slice warm_key("f");
    bool exact_match = false;
    ASSERT_OK(_iter->seek_at_or_after(&warm_key, &exact_match));
    ASSERT_TRUE(exact_match);
    ASSERT_EQ(2, _iter->get_current_ordinal());
    const auto reads_before = _stats.io_count_request;
    const auto pages_before = _stats.total_pages_num;

    for (int i = 0; i < 1000; ++i) {
        std::string value = "z" + std::to_string(i);
        Slice key(value);
        ASSERT_TRUE(_iter->seek_at_or_after(&key, &exact_match).is_not_found());
    }

    // Even with the page cache disabled, ordinary misses reuse the parsed page.
    EXPECT_EQ(reads_before, _stats.io_count_request);
    EXPECT_EQ(pages_before, _stats.total_pages_num);

    // Re-seeking the old offset can take ParsedPage's no-op path. Its decoder
    // must still return the correct suffix, including a read past the last page.
    ASSERT_OK(_iter->seek_to_ordinal(2));
    auto column = BinaryColumn::create();
    size_t count = 3;
    ASSERT_OK(_iter->next_batch(&count, column.get()));
    ASSERT_EQ(2, count);
    ASSERT_EQ(2, column->size());
    EXPECT_EQ("f", column->get_slice(0).to_string());
    EXPECT_EQ("h", column->get_slice(1).to_string());
    EXPECT_EQ(reads_before, _stats.io_count_request);

    // Exercise another miss with the page exhausted, then value and ordinal
    // seeks back into the terminal page without loading it again.
    Slice missing_key("z");
    ASSERT_TRUE(_iter->seek_at_or_after(&missing_key, &exact_match).is_not_found());
    ASSERT_NO_FATAL_FAILURE(check_seek("h"));
    ASSERT_OK(_iter->seek_to_ordinal(2));
    column->reset_column();
    count = 1;
    ASSERT_OK(_iter->next_batch(&count, column.get()));
    ASSERT_EQ(1, count);
    EXPECT_EQ("f", column->get_slice(0).to_string());
    EXPECT_EQ(reads_before, _stats.io_count_request);
    EXPECT_EQ(pages_before, _stats.total_pages_num);

    // Backward seeking and subsequent cross-page reads remain correct.
    ASSERT_NO_FATAL_FAILURE(check_seek("b"));
}

INSTANTIATE_TEST_SUITE_P(PageLayouts, IndexedColumnEndMissTest, testing::Bool());

} // namespace starrocks
