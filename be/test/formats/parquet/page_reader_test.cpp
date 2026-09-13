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

#include "formats/parquet/page_reader.h"

#include <gtest/gtest.h>
#include <parquet/encryption/encryption.h>

#include <iostream>

#include "base/coding.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/util/thrift_util.h"
#include "connector/hive/scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "fs/fs_memory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/parquet_types.h"
#include "io/string_input_stream.h"
namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() = default;
    ~ParquetPageReaderTest() override = default;
};

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    FormatScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    reader.seek_to_offset(total_size);
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetPageReaderTest, ExtraBytes) {
    std::string buffer;
    FormatScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }
    size_t page_2_size = buffer.size();

    size_t extra_nbytes = 10;
    buffer.resize(buffer.size() + extra_nbytes);
    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    ASSERT_TRUE(reader.seek_to_offset(page_2_size).ok());
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

// Parquet Modular Encryption: an encrypted page header is stored as [4-byte len][nonce][ct][tag].
// That length prefix sits outside the AEAD, so it is unauthenticated attacker-controlled input --
// a corrupt or hostile file can name any size at all, and it is read before anything can be
// verified. These tests drive next_header() straight at the bound; none of them needs valid
// ciphertext, because a refusal has to happen before the length is ever used to size a buffer.
class ParquetEncryptedPageHeaderTest : public testing::Test {
protected:
    // Minimal encrypted-file state: enough for the PageReader to take the PME path and build its
    // decryptors. AES_GCM_V1 with a 32-byte footer key, matching what Iceberg PME writes.
    static FileMetaData encrypted_metadata() {
        FileMetaData meta;
        FileMetaData::EncryptionContext ctx;
        ctx.aad_file_unique = "aad-file-unique";
        ctx.aad_prefix = "";
        ctx.algorithm = static_cast<int32_t>(::parquet::ParquetCipher::AES_GCM_V1);
        meta.set_encryption_ctx(std::move(ctx));
        return meta;
    }

    // The per-file DEK travels on the scan range, not on the cached FileMetaData.
    static TParquetEncryptionInfo scan_range_key() {
        TParquetEncryptionInfo enc;
        enc.__set_file_dek(std::string(32, '\x2b'));
        return enc;
    }

    // A chunk holding nothing but a 4-byte length prefix claiming `claimed_len` bytes follow.
    static Status read_header_with_claimed_length(uint32_t claimed_len, size_t chunk_size) {
        std::string buffer(chunk_size, '\0');
        encode_fixed32_le(reinterpret_cast<uint8_t*>(buffer.data()), claimed_len);

        RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
        SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

        FormatScannerStats stats;
        FileMetaData meta = encrypted_metadata();
        TParquetEncryptionInfo enc = scan_range_key();
        ColumnReaderOptions opts;
        opts.stats = &stats;
        opts.file_meta_data = &meta;
        opts.parquet_encryption_info = &enc;

        PageReader reader(&stream, 0, chunk_size, /*num_values=*/10, opts, tparquet::CompressionCodec::UNCOMPRESSED);
        return reader.next_header();
    }
};

// The DEK must reach the reader from the scan range on every scan. It used to be cached inside
// FileMetaData::EncryptionContext, which goes into the process-wide footer cache -- so a cache hit
// decrypted the file with the cached key and never consulted the planner at all. With the key no
// longer cached, an encrypted file with no planner-supplied key has to fail here, unconditionally.
TEST_F(ParquetEncryptedPageHeaderTest, EncryptedFileWithNoPlannerKeyIsRefused) {
    std::string buffer(64, '\0');
    encode_fixed32_le(reinterpret_cast<uint8_t*>(buffer.data()), 32);
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    FormatScannerStats stats;
    FileMetaData meta = encrypted_metadata();
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.file_meta_data = &meta;
    opts.parquet_encryption_info = nullptr; // planner sent no key

    PageReader reader(&stream, 0, 64, /*num_values=*/10, opts, tparquet::CompressionCodec::UNCOMPRESSED);
    Status st = reader.next_header();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("no decryption key was provided by the planner") != std::string::npos)
            << st.to_string();
}

// Same, for a key that is present but empty -- an old FE, or a bug, must not read as "plaintext".
TEST_F(ParquetEncryptedPageHeaderTest, EncryptedFileWithEmptyPlannerKeyIsRefused) {
    std::string buffer(64, '\0');
    encode_fixed32_le(reinterpret_cast<uint8_t*>(buffer.data()), 32);
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    FormatScannerStats stats;
    FileMetaData meta = encrypted_metadata();
    TParquetEncryptionInfo enc;
    enc.__set_file_dek("");
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.file_meta_data = &meta;
    opts.parquet_encryption_info = &enc;

    PageReader reader(&stream, 0, 64, /*num_values=*/10, opts, tparquet::CompressionCodec::UNCOMPRESSED);
    Status st = reader.next_header();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("no decryption key was provided by the planner") != std::string::npos)
            << st.to_string();
}

TEST_F(ParquetEncryptedPageHeaderTest, LengthBeyondTheColumnChunkIsRefused) {
    // 4 GB claimed inside a 64-byte chunk. Without the bound this sized the header buffer before
    // read_at_fully() could discover the header runs past the chunk, so a 64-byte file could ask the
    // BE for a multi-gigabyte allocation.
    Status st = read_header_with_claimed_length(0xFFFFFF00, /*chunk_size=*/64);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("invalid encrypted Parquet page-header length") != std::string::npos)
            << st.to_string();
}

TEST_F(ParquetEncryptedPageHeaderTest, LengthAboveTheHeaderCapIsRefused) {
    // Within the chunk but past kMaxPageHeaderSize (16 MB), the same cap the plaintext header path
    // applies. A large but self-consistent file must not lift the bound.
    const size_t chunk_size = 32 * 1024 * 1024;
    Status st = read_header_with_claimed_length(20 * 1024 * 1024, chunk_size);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("invalid encrypted Parquet page-header length") != std::string::npos)
            << st.to_string();
}

TEST_F(ParquetEncryptedPageHeaderTest, ZeroLengthIsRefused) {
    // A zero-length module has no nonce and no tag, so there is nothing to authenticate. Passing it
    // to Decrypt() reaches arrow with an empty span rather than being rejected here.
    Status st = read_header_with_claimed_length(0, /*chunk_size=*/64);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("invalid encrypted Parquet page-header length") != std::string::npos)
            << st.to_string();
}

TEST_F(ParquetEncryptedPageHeaderTest, LengthThatOverflowsTheTotalIsRefused) {
    // The four largest uint32 values wrap `4 + module_len`: 0xFFFFFFFF becomes 3 in 32-bit arithmetic,
    // which is under both bounds. The bound has to be computed wide enough that a hostile prefix cannot
    // arrive at a small total, so these are checked one by one rather than as a representative value.
    for (uint32_t module_len : {0xFFFFFFFCu, 0xFFFFFFFDu, 0xFFFFFFFEu, 0xFFFFFFFFu}) {
        Status st = read_header_with_claimed_length(module_len, /*chunk_size=*/64);
        ASSERT_TRUE(st.is_corruption()) << "module_len=" << module_len << ": " << st.to_string();
        ASSERT_TRUE(st.to_string().find("invalid encrypted Parquet page-header length") != std::string::npos)
                << "module_len=" << module_len << ": " << st.to_string();
    }
}

TEST_F(ParquetEncryptedPageHeaderTest, LengthExactlyFillingTheChunkPassesTheBound) {
    // The bound is on the length, not on whether the bytes decrypt: a prefix that fits the chunk
    // exactly must get past it and fail later in the AEAD instead. Guards against tightening the
    // comparison to `>=` and rejecting a legitimate final page.
    const size_t chunk_size = 64;
    Status st = read_header_with_claimed_length(chunk_size - 4, chunk_size);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("invalid encrypted Parquet page-header length") == std::string::npos)
            << "a length that fits the chunk must be rejected by decryption, not by the length bound: "
            << st.to_string();
}

} // namespace starrocks::parquet
