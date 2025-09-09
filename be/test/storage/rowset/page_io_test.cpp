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

#include "storage/rowset/page_io.h"

#include <gtest/gtest.h>

#include "cache/lrucache_engine.h"
#include "cache/object_cache/page_cache.h"
#include "fs/fs_memory.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/bitshuffle_page.h"
#include "testutil/assert.h"
#include "util/compression/block_compression.h"

namespace starrocks {

class PageIOTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    OwnedSlice _build_data_page(const std::vector<int32_t>& values) const;
    void _compress_data_page(const Slice& slice, faststring* compressed_body);
    PageFooterPB _build_page_footer(uint64_t uncompressed_size);
    PageReadOptions _build_read_options(io::SeekableInputStream*, size_t offset, size_t size, bool use_page_cache);

    size_t _cache_size = 10 * 1024 * 1024;
    std::shared_ptr<StoragePageCache> _prev_page_cache;

    std::shared_ptr<LRUCacheEngine> _lru_cache;
    std::shared_ptr<StoragePageCache> _page_cache;

    std::shared_ptr<MemoryFileSystem> _fs;
    const BlockCompressionCodec* _codec = nullptr;
    OlapReaderStatistics _stats;
};

void PageIOTest::SetUp() {
    _prev_page_cache = DataCache::GetInstance()->page_cache_ptr();
    MemCacheOptions options{.mem_space_size = _cache_size};
    _lru_cache = std::make_shared<LRUCacheEngine>();
    ASSERT_OK(_lru_cache->init(options));
    _page_cache = std::make_shared<StoragePageCache>(_lru_cache.get());
    DataCache::GetInstance()->set_page_cache(_page_cache);

    _fs = std::make_shared<MemoryFileSystem>();
    ASSERT_OK(get_block_compression_codec(CompressionTypePB::LZ4, &_codec));
}

void PageIOTest::TearDown() {
    DataCache::GetInstance()->set_page_cache(_prev_page_cache);
}

OwnedSlice PageIOTest::_build_data_page(const std::vector<int32_t>& values) const {
    PageBuilderOptions options;
    options.data_page_size = 256 * 1024;
    PageBuilderOptions build_opts;
    BitshufflePageBuilder<TYPE_INT> builder(build_opts);
    builder.add((const uint8_t*)values.data(), 3);
    return builder.finish()->build();
}

void PageIOTest::_compress_data_page(const Slice& slice, faststring* compressed_body) {
    std::vector<Slice> src_slices{slice};
    ASSERT_OK(PageIO::compress_page_body(_codec, 0, src_slices, compressed_body));
    uint64_t uncompressed_size = slice.size;
    uint64_t compressed_size = compressed_body->size();
    ASSERT_LT(compressed_size, uncompressed_size);
}

PageFooterPB PageIOTest::_build_page_footer(uint64_t uncompressed_size) {
    PageFooterPB footer;
    footer.set_type(DATA_PAGE);
    auto* data_page_footer = footer.mutable_data_page_footer();
    data_page_footer->set_nullmap_size(0);
    footer.set_uncompressed_size(uncompressed_size);

    return footer;
}

PageReadOptions PageIOTest::_build_read_options(io::SeekableInputStream* file, size_t offset, size_t size,
                                                bool use_page_cache) {
    PageReadOptions read_opts;
    read_opts.page_pointer.offset = offset;
    read_opts.page_pointer.size = size;
    read_opts.codec = _codec;
    read_opts.stats = &_stats;
    read_opts.encoding_type = EncodingTypePB::BIT_SHUFFLE;
    read_opts.use_page_cache = use_page_cache;
    read_opts.read_file = file;

    return read_opts;
}

TEST_F(PageIOTest, test_use_no_cache_compress) {
    std::vector<int32_t> values{1, 2, 3};
    OwnedSlice page = _build_data_page(values);
    size_t uncompressed_size = page.slice().size;

    // compress
    faststring compressed_body;
    _compress_data_page(page.slice(), &compressed_body);

    // page footer
    PageFooterPB footer = _build_page_footer(uncompressed_size);

    // write
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_use_no_cache_compress"));

    PagePointer result;
    std::vector<Slice> compressed_page{compressed_body};
    ASSERT_OK(PageIO::write_page(write_file.get(), compressed_page, footer, &result));
    ASSERT_OK(write_file->close());
    uint64_t file_size = write_file->size();

    // read
    ASSIGN_OR_ASSERT_FAIL(auto read_file, _fs->new_random_access_file("/test_use_no_cache_compress"));

    PageReadOptions read_opts = _build_read_options(read_file.get(), 0, file_size, false);

    PageHandle handle;
    Slice body;
    PageFooterPB read_footer;
    ASSERT_OK(PageIO::read_and_decompress_page(read_opts, &handle, &body, &read_footer));
    ASSERT_EQ(_stats.uncompressed_bytes_read, uncompressed_size + footer.ByteSizeLong() + 4);

    // check
    BitShufflePageDecoder<TYPE_INT> decoder(body);
    ASSERT_OK(decoder.init());

    size_t size = 10;
    Int32Column col;
    ASSERT_OK(decoder.next_batch(&size, &col));
    ASSERT_EQ(col.debug_string(), "[1, 2, 3]");
}

TEST_F(PageIOTest, test_use_no_cache_uncompress) {
    std::vector<int32_t> values{1, 2, 3};
    OwnedSlice page = _build_data_page(values);
    size_t uncompressed_size = page.slice().size;

    // page footer
    PageFooterPB footer = _build_page_footer(uncompressed_size);

    // write
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_use_no_cache"));

    PagePointer result;
    std::vector<Slice> compressed_page{page.slice()};
    ASSERT_OK(PageIO::write_page(write_file.get(), compressed_page, footer, &result));
    ASSERT_OK(write_file->close());
    uint64_t file_size = write_file->size();

    // read
    ASSIGN_OR_ASSERT_FAIL(auto read_file, _fs->new_random_access_file("/test_use_no_cache"));

    PageReadOptions read_opts = _build_read_options(read_file.get(), 0, file_size, false);

    PageHandle handle;
    Slice body;
    PageFooterPB read_footer;
    ASSERT_OK(PageIO::read_and_decompress_page(read_opts, &handle, &body, &read_footer));
    ASSERT_EQ(_stats.uncompressed_bytes_read, uncompressed_size + footer.ByteSizeLong() + 4);

    // check
    BitShufflePageDecoder<TYPE_INT> decoder(body);
    ASSERT_OK(decoder.init());

    size_t size = 10;
    Int32Column col;
    ASSERT_OK(decoder.next_batch(&size, &col));
    ASSERT_EQ(col.debug_string(), "[1, 2, 3]");
}

TEST_F(PageIOTest, test_use_cache_hit) {
    std::vector<int32_t> values{1, 2, 3};
    OwnedSlice page = _build_data_page(values);
    size_t uncompressed_size = page.slice().size;

    // compress
    faststring compressed_body;
    _compress_data_page(page.slice(), &compressed_body);

    // page footer
    PageFooterPB footer = _build_page_footer(uncompressed_size);

    // write
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_use_cache_hit"));

    PagePointer result;
    std::vector<Slice> compressed_page{compressed_body};
    ASSERT_OK(PageIO::write_page(write_file.get(), compressed_page, footer, &result));
    ASSERT_OK(write_file->close());
    uint64_t file_size = write_file->size();

    // read
    ASSIGN_OR_ASSERT_FAIL(auto read_file, _fs->new_random_access_file("/test_use_cache_hit"));
    PageReadOptions read_opts = _build_read_options(read_file.get(), 0, file_size, true);

    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.uncompressed_bytes_read, uncompressed_size + footer.ByteSizeLong() + 4);
        ASSERT_EQ(_stats.cached_pages_num, 0);
    }

    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.uncompressed_bytes_read, uncompressed_size + footer.ByteSizeLong() + 4);
        ASSERT_EQ(_stats.cached_pages_num, 1);

        // check
        BitShufflePageDecoder<TYPE_INT> decoder(body);
        ASSERT_OK(decoder.init());

        size_t size = 10;
        Int32Column col;
        ASSERT_OK(decoder.next_batch(&size, &col));
        ASSERT_EQ(col.debug_string(), "[1, 2, 3]");
    }
}
} // namespace starrocks
