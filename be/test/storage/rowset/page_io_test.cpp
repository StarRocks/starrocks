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

#include "base/compression/block_compression.h"
#include "base/testutil/assert.h"
#include "cache/datacache.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "fs/bundle_file.h"
#include "fs/fs_memory.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/bitshuffle_page.h"

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

// When use_page_cache = false, PageIO should not insert the page into StoragePageCache.
// Otherwise a later read with use_page_cache = true may hit a stale page from cache.
TEST_F(PageIOTest, test_no_cache_not_insert_into_cache) {
    std::vector<int32_t> values{1, 2, 3};
    OwnedSlice page = _build_data_page(values);
    size_t uncompressed_size = page.slice().size;

    // write an uncompressed page
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_no_cache_not_insert_into_cache"));

    PageFooterPB footer = _build_page_footer(uncompressed_size);
    PagePointer result;
    std::vector<Slice> page_body{page.slice()};
    ASSERT_OK(PageIO::write_page(write_file.get(), page_body, footer, &result));
    ASSERT_OK(write_file->close());
    uint64_t file_size = write_file->size();

    // first read with use_page_cache = false, this must NOT insert into cache
    ASSIGN_OR_ASSERT_FAIL(auto read_file, _fs->new_random_access_file("/test_no_cache_not_insert_into_cache"));
    PageReadOptions read_opts_no_cache = _build_read_options(read_file.get(), 0, file_size, false);
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_no_cache, &handle, &body, &read_footer));
    }

    // second read with use_page_cache = true.
    // If the first read had incorrectly inserted the page into cache, this read would hit cache
    // and increase cached_pages_num to 1. With the correct behavior, cached_pages_num should remain 0.
    PageReadOptions read_opts_use_cache = _build_read_options(read_file.get(), 0, file_size, true);
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_use_cache, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.cached_pages_num, 0);
    }
}

TEST_F(PageIOTest, test_corrupted_cache) {
    // open file
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_corrupt"));

    // corrupt file with no footer
    std::vector<int32_t> values{1, 2, 3};
    OwnedSlice page = _build_data_page(values);
    ASSERT_OK(write_file->append(page.slice()));
    ASSERT_OK(write_file->close());
    uint64_t file_size = write_file->size();

    // read
    ASSIGN_OR_ASSERT_FAIL(auto read_file, _fs->new_random_access_file("/test_corrupt"));
    PageReadOptions read_opts = _build_read_options(read_file.get(), 0, file_size, false);
    PageHandle handle;
    Slice body;
    PageFooterPB read_footer;
    Status s = PageIO::read_and_decompress_page(read_opts, &handle, &body, &read_footer);
    ASSERT_TRUE(s.is_corruption());
}

// Two slices of the same bundled data file must not collide in the page cache.
//
// Pre-fix, the page cache key was (filename, page_pointer.offset). page_pointer.offset is
// stream-relative for BundleSeekableInputStream, so two slices at different bundle offsets
// produced identical keys at equal intra-slice positions. A cross-boundary UPSERT that
// packs multiple child tablets' writes into one physical file (and a subsequent MERGE that
// collects those slices into one tablet) therefore made slice B return slice A's cached
// bytes — silently duplicating slice A and erasing slice B from query results.
//
// Fix: mix in the stream's bundle_file_offset so the key names the page's unique absolute
// position in the physical file. This test writes two distinct pages back-to-back into one
// file, exposes them via BundleSeekableInputStream wrappers at offsets 0 and page_a_size,
// and confirms a cache warm on slice A does not bleed into slice B.
TEST_F(PageIOTest, test_bundle_slices_do_not_collide_in_page_cache) {
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_file, _fs->new_writable_file(write_opts, "/test_bundle_slices"));

    PagePointer result_a;
    std::vector<int32_t> values_a{1, 2, 3};
    OwnedSlice page_a = _build_data_page(values_a);
    PageFooterPB footer_a = _build_page_footer(page_a.slice().size);
    std::vector<Slice> page_a_body{page_a.slice()};
    ASSERT_OK(PageIO::write_page(write_file.get(), page_a_body, footer_a, &result_a));
    uint64_t slice_a_size = write_file->size();

    PagePointer result_b;
    std::vector<int32_t> values_b{10, 20, 30};
    OwnedSlice page_b = _build_data_page(values_b);
    PageFooterPB footer_b = _build_page_footer(page_b.slice().size);
    std::vector<Slice> page_b_body{page_b.slice()};
    ASSERT_OK(PageIO::write_page(write_file.get(), page_b_body, footer_b, &result_b));
    ASSERT_OK(write_file->close());
    const uint64_t slice_b_size = write_file->size() - slice_a_size;
    ASSERT_GT(slice_a_size, 0u);
    ASSERT_GT(slice_b_size, 0u);

    ASSIGN_OR_ASSERT_FAIL(auto raw_a, _fs->new_random_access_file("/test_bundle_slices"));
    ASSIGN_OR_ASSERT_FAIL(auto raw_b, _fs->new_random_access_file("/test_bundle_slices"));
    auto slice_a = std::make_shared<BundleSeekableInputStream>(raw_a->stream(), /*offset=*/0, slice_a_size);
    auto slice_b = std::make_shared<BundleSeekableInputStream>(raw_b->stream(),
                                                               /*offset=*/slice_a_size, slice_b_size);
    ASSERT_OK(slice_a->init());
    ASSERT_OK(slice_b->init());

    // Sanity: the two slices must produce different page-cache keys for the same stream-relative
    // offset; otherwise the fix being tested is not even exercised.
    ASSERT_NE(slice_a->page_cache_key(0), slice_b->page_cache_key(0));

    // Warm the cache with slice A's page at stream-relative offset 0.
    PageReadOptions read_opts_a = _build_read_options(slice_a.get(), 0, slice_a_size, true);
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_a, &handle, &body, &read_footer));
    }
    {
        // second read is a cache hit on slice A
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_a, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.cached_pages_num, 1);
    }

    // Now read slice B at stream-relative offset 0. The stream's bundle_file_offset is
    // slice_a_size, so the key must differ from slice A's — otherwise this lookup hits slice
    // A's cached bytes and decodes `{1,2,3}` instead of `{10,20,30}`.
    PageReadOptions read_opts_b = _build_read_options(slice_b.get(), 0, slice_b_size, true);
    const auto cached_before = _stats.cached_pages_num;
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_b, &handle, &body, &read_footer));

        // This must be a cache miss: the cache key included slice B's bundle offset.
        ASSERT_EQ(_stats.cached_pages_num, cached_before);

        // And the decoded bytes must be slice B's content, not slice A's.
        BitShufflePageDecoder<TYPE_INT> decoder(body);
        ASSERT_OK(decoder.init());
        size_t size = 10;
        Int32Column col;
        ASSERT_OK(decoder.next_batch(&size, &col));
        ASSERT_EQ(col.debug_string(), "[10, 20, 30]");
    }
}

// Production wraps a BundleSeekableInputStream inside a RandomAccessFile carrying the physical
// path, so the slice base offset is held by an inner stream the outer wrapper does not consult
// when producing a key. RandomAccessFile must therefore carry the bundle offset itself and fold
// it into page_cache_key — otherwise two slices of the same physical file produce identical
// (path, stream_offset) keys at the same intra-slice offset and collide in the page cache.
TEST_F(PageIOTest, test_random_access_file_carries_bundle_offset_into_cache_key) {
    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto wf, _fs->new_writable_file(write_opts, "/bundle_via_rfile"));
    const std::string payload_a(1024, 'A');
    const std::string payload_b(2048, 'B');
    ASSERT_OK(wf->append(payload_a));
    ASSERT_OK(wf->append(payload_b));
    ASSERT_OK(wf->close());

    ASSIGN_OR_ASSERT_FAIL(auto raw_a, _fs->new_random_access_file("/bundle_via_rfile"));
    ASSIGN_OR_ASSERT_FAIL(auto raw_b, _fs->new_random_access_file("/bundle_via_rfile"));
    auto bundle_a = std::make_shared<BundleSeekableInputStream>(raw_a->stream(), /*offset=*/0, payload_a.size());
    auto bundle_b = std::make_shared<BundleSeekableInputStream>(raw_b->stream(),
                                                                /*offset=*/payload_a.size(), payload_b.size());

    RandomAccessFile rf_a(std::move(bundle_a), "/bundle_via_rfile", /*is_cache_hit=*/false, /*bundle_offset=*/0);
    RandomAccessFile rf_b(std::move(bundle_b), "/bundle_via_rfile", /*is_cache_hit=*/false,
                          /*bundle_offset=*/static_cast<int64_t>(payload_a.size()));

    ASSERT_EQ(rf_a.filename(), rf_b.filename());
    ASSERT_NE(rf_a.page_cache_key(0), rf_b.page_cache_key(0));
}

// Two distinct physical files must produce different page-cache keys at the same offset,
// even though most concrete SeekableInputStream subclasses (S3InputStream, MemoryFileInputStream,
// HdfsInputStream, ...) leave the inherited _filename member empty and only override the virtual
// filename(). A naive default that reads _filename would collapse every file's keys to
// (empty, offset) and let production page-cache lookups for file A return file B's bytes.
TEST_F(PageIOTest, test_distinct_files_do_not_collide_in_page_cache) {
    std::vector<int32_t> values_a{1, 2, 3};
    std::vector<int32_t> values_b{10, 20, 30};
    OwnedSlice page_a = _build_data_page(values_a);
    OwnedSlice page_b = _build_data_page(values_b);
    PageFooterPB footer_a = _build_page_footer(page_a.slice().size);
    PageFooterPB footer_b = _build_page_footer(page_b.slice().size);

    WritableFileOptions write_opts;
    write_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ASSERT_FAIL(auto write_a, _fs->new_writable_file(write_opts, "/distinct_files_A"));
    PagePointer result_a;
    std::vector<Slice> page_a_body{page_a.slice()};
    ASSERT_OK(PageIO::write_page(write_a.get(), page_a_body, footer_a, &result_a));
    ASSERT_OK(write_a->close());
    uint64_t file_a_size = write_a->size();

    ASSIGN_OR_ASSERT_FAIL(auto write_b, _fs->new_writable_file(write_opts, "/distinct_files_B"));
    PagePointer result_b;
    std::vector<Slice> page_b_body{page_b.slice()};
    ASSERT_OK(PageIO::write_page(write_b.get(), page_b_body, footer_b, &result_b));
    ASSERT_OK(write_b->close());
    uint64_t file_b_size = write_b->size();

    ASSIGN_OR_ASSERT_FAIL(auto read_a, _fs->new_random_access_file("/distinct_files_A"));
    ASSIGN_OR_ASSERT_FAIL(auto read_b, _fs->new_random_access_file("/distinct_files_B"));

    // Pre-condition: the wrappers report distinct names. If they didn't, the test below could
    // trivially pass for a different reason.
    ASSERT_NE(read_a->filename(), read_b->filename());

    // The keys must differ at the same stream-relative offset 0; otherwise file B's lookup at
    // offset 0 would hit file A's cached bytes in production. page_cache_key must dispatch
    // through the virtual filename() so it picks up RandomAccessFile's _name rather than the
    // empty _filename member buried on the underlying stream.
    ASSERT_NE(read_a->page_cache_key(0), read_b->page_cache_key(0));

    // End-to-end: warm the cache from file A, then read file B at the same offset and confirm
    // we decode B's bytes (not A's).
    PageReadOptions read_opts_a = _build_read_options(read_a.get(), 0, file_a_size, true);
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_a, &handle, &body, &read_footer));
    }
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_a, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.cached_pages_num, 1);
    }

    PageReadOptions read_opts_b = _build_read_options(read_b.get(), 0, file_b_size, true);
    const auto cached_before = _stats.cached_pages_num;
    {
        PageHandle handle;
        Slice body;
        PageFooterPB read_footer;
        ASSERT_OK(PageIO::read_and_decompress_page(read_opts_b, &handle, &body, &read_footer));
        ASSERT_EQ(_stats.cached_pages_num, cached_before);

        BitShufflePageDecoder<TYPE_INT> decoder(body);
        ASSERT_OK(decoder.init());
        size_t size = 10;
        Int32Column col;
        ASSERT_OK(decoder.next_batch(&size, &col));
        ASSERT_EQ(col.debug_string(), "[10, 20, 30]");
    }
}

} // namespace starrocks
