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

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/writer.h>
#include <parquet/encryption/encryption.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "exprs/expr_context.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {

// Reads Parquet Modular Encryption files written by Arrow's own public writer
// (parquet::ParquetFileWriter + FileEncryptionProperties) back through StarRocks' reader. The writer
// here is independent of StarRocks' Iceberg write path, so these cases check interoperability with a
// spec-conforming producer -- what reading a table another engine encrypted depends on -- and cover
// the encrypted footer, dictionary-page and data-page header and page decryption end to end.
class EncryptedFileReaderTest : public testing::Test {
protected:
    static constexpr int kNumRows = 2000;
    // 16-byte AES key: the length Iceberg's encryption.data-key-length defaults to.
    const std::string kDek = std::string("0123456789abcdef");
    const std::string kAadPrefix = std::string("iceberg-file-aad");

    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }

    struct WriteSpec {
        ::parquet::ParquetCipher::type cipher = ::parquet::ParquetCipher::AES_GCM_V1;
        bool store_aad_prefix = false;
        bool write_page_index = false;
    };

    // Writes kNumRows rows of (c_int INT32, c_str UTF8) with a small page size, so each column chunk
    // spans several data pages after a dictionary page and the page ordinals in the AAD advance.
    void write_encrypted_file(const WriteSpec& spec) {
        auto node = ::parquet::schema::GroupNode::Make(
                "schema", ::parquet::Repetition::REQUIRED,
                {::parquet::schema::PrimitiveNode::Make("c_int", ::parquet::Repetition::REQUIRED,
                                                        ::parquet::Type::INT32),
                 ::parquet::schema::PrimitiveNode::Make("c_str", ::parquet::Repetition::REQUIRED,
                                                        ::parquet::Type::BYTE_ARRAY, ::parquet::ConvertedType::UTF8)});
        auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(node);

        ::parquet::FileEncryptionProperties::Builder enc_builder{::arrow::util::SecureString(std::string(kDek))};
        enc_builder.algorithm(spec.cipher)->aad_prefix(kAadPrefix);
        if (!spec.store_aad_prefix) {
            enc_builder.disable_aad_prefix_storage();
        }
        ::parquet::WriterProperties::Builder props_builder;
        props_builder.data_pagesize(1024)->encryption(enc_builder.build());
        if (spec.write_page_index) {
            props_builder.enable_write_page_index();
        }
        auto props = props_builder.build();

        auto sink = ::arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = ::parquet::ParquetFileWriter::Open(sink, schema, props);
        auto* rg = writer->AppendRowGroup();

        std::vector<int32_t> ints(kNumRows);
        for (int i = 0; i < kNumRows; ++i) {
            ints[i] = i;
        }
        static_cast<::parquet::Int32Writer*>(rg->NextColumn())->WriteBatch(kNumRows, nullptr, nullptr, ints.data());

        // A handful of distinct values, so the string column is dictionary-encoded and has a
        // dictionary page ahead of its data pages.
        std::vector<std::string> strs(kNumRows);
        std::vector<::parquet::ByteArray> byte_arrays(kNumRows);
        for (int i = 0; i < kNumRows; ++i) {
            strs[i] = "value-" + std::to_string(i % 7);
            byte_arrays[i] = ::parquet::ByteArray(static_cast<uint32_t>(strs[i].size()),
                                                  reinterpret_cast<const uint8_t*>(strs[i].data()));
        }
        static_cast<::parquet::ByteArrayWriter*>(rg->NextColumn())
                ->WriteBatch(kNumRows, nullptr, nullptr, byte_arrays.data());
        rg->Close();
        writer->Close();

        auto buffer = sink->Finish().ValueOrDie();
        ASSIGN_OR_ABORT(auto out, _fs.new_writable_file(_file_path));
        ASSERT_OK(out->append(Slice(buffer->data(), buffer->size())));
        ASSERT_OK(out->close());
    }

    // Reads every row back with the given planner-supplied key material. nullptr means "no
    // encryption info on the scan range", as for a plaintext table.
    //
    // `c_int_ge`, when set, adds the predicate `c_int >= value` with page-index pruning on, so the reader
    // has to decrypt the ColumnIndex and OffsetIndex modules to pick the pages to read.
    StatusOr<ChunkPtr> read_all(const TParquetEncryptionInfo* enc_info, std::optional<int> c_int_ge = std::nullopt) {
        // The chunk below must register its columns under these same slot ids: the reader fills columns
        // by slot id, not by position.
        constexpr SlotId kIntSlot = 1;
        constexpr SlotId kStrSlot = 2;
        std::vector<Utils::SlotDesc> slot_descs = {{"c_int", TypeDescriptor::from_logical_type(TYPE_INT), kIntSlot},
                                                   {"c_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR), kStrSlot},
                                                   {""}};
        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());

        auto* ctx = _pool.add(new HdfsScannerContext());
        ctx->format_scan_context.lazy_column_coalesce_counter = &_lazy_column_coalesce_counter;
        Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
        ctx->slot_descs = tuple_desc->slots();

        ASSIGN_OR_RETURN(auto file_size, _fs.get_file_size(_file_path));
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = _file_path;
        scan_range->file_length = file_size;
        scan_range->offset = 4;
        scan_range->length = file_size;
        if (enc_info != nullptr) {
            scan_range->__set_parquet_encryption_info(*enc_info);
        }
        ctx->scan_range = scan_range;
        ctx->format_scan_context.parquet_encryption_info =
                scan_range->__isset.parquet_encryption_info ? &scan_range->parquet_encryption_info : nullptr;
        ctx->format_scan_context.scan_range_offset = scan_range->offset;
        ctx->format_scan_context.scan_range_length = scan_range->length;
        ctx->format_scan_context.timezone = "UTC";
        ctx->format_scan_context.stats = &_stats;
        ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
        if (c_int_ge.has_value()) {
            std::vector<TExpr> t_conjuncts;
            ParquetUTBase::append_int_conjunct(TExprOpcode::GE, kIntSlot, *c_int_ge, &t_conjuncts);
            std::vector<ExprContext*> expr_ctxs;
            ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &expr_ctxs);
            ctx->format_scan_context.conjunct_ctxs_by_slot.insert({kIntSlot, expr_ctxs});
            ctx->format_scan_context.options.parquet_page_index_enable = true;
            ParquetUTBase::setup_conjuncts_manager(ctx->format_scan_context.conjunct_ctxs_by_slot[kIntSlot], nullptr,
                                                   tuple_desc, _runtime_state, ctx);
        }

        ASSIGN_OR_RETURN(auto file, _fs.new_random_access_file(_file_path));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), file_size);
        RETURN_IF_ERROR(file_reader->init(&ctx->format_scan_context));

        auto make_chunk = [&]() {
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true),
                                 kIntSlot);
            chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true),
                                 kStrSlot);
            return chunk;
        };
        ChunkPtr all = make_chunk();
        while (true) {
            ChunkPtr chunk = make_chunk();
            Status st = file_reader->get_next(&chunk);
            if (st.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(st);
            if (chunk->num_rows() == 0) {
                break;
            }
            all->append(*chunk);
        }
        return all;
    }

    TParquetEncryptionInfo key_info(bool with_aad_prefix) const {
        TParquetEncryptionInfo info;
        info.__set_file_dek(kDek);
        if (with_aad_prefix) {
            info.__set_aad_prefix(kAadPrefix);
        }
        return info;
    }

    static void expect_rows(const ChunkPtr& chunk) {
        ASSERT_EQ(kNumRows, chunk->num_rows());
        for (int i : {0, 1, 999, kNumRows - 1}) {
            EXPECT_EQ(std::to_string(i), chunk->get_column_by_index(0)->debug_item(i));
            EXPECT_EQ("'value-" + std::to_string(i % 7) + "'", chunk->get_column_by_index(1)->debug_item(i));
        }
    }

    FormatScannerStats _stats;
    MemoryFileSystem _fs;
    std::string _file_path{"/encrypted.parquet"};
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
    std::atomic<int> _lazy_column_coalesce_counter = 0;
};

// Iceberg's layout: the writer withholds the AAD prefix from the file and the planner supplies it from
// key_metadata, together with the DEK.
TEST_F(EncryptedFileReaderTest, AesGcmWithWithheldAadPrefixIsReadable) {
    write_encrypted_file({});
    const auto enc = key_info(/*with_aad_prefix=*/true);
    ASSIGN_OR_ABORT(auto chunk, read_all(&enc));
    expect_rows(chunk);
}

// AES_GCM_CTR_V1 encrypts page data with CTR and everything else with GCM, so the page and the
// metadata decryptors must be built with the right cipher each.
TEST_F(EncryptedFileReaderTest, AesGcmCtrIsReadable) {
    write_encrypted_file({.cipher = ::parquet::ParquetCipher::AES_GCM_CTR_V1});
    const auto enc = key_info(/*with_aad_prefix=*/true);
    ASSIGN_OR_ABORT(auto chunk, read_all(&enc));
    expect_rows(chunk);
}

// A writer that stores the AAD prefix in the file: the planner need only supply the DEK.
TEST_F(EncryptedFileReaderTest, AadPrefixStoredInTheFileIsUsed) {
    write_encrypted_file({.store_aad_prefix = true});
    const auto enc = key_info(/*with_aad_prefix=*/false);
    ASSIGN_OR_ABORT(auto chunk, read_all(&enc));
    expect_rows(chunk);
}

// The page index is encrypted as two modules of its own (ColumnIndex, OffsetIndex). With a selective
// predicate the reader decrypts both to choose pages, and must still return exactly the matching rows.
TEST_F(EncryptedFileReaderTest, EncryptedPageIndexDrivesPagePruning) {
    write_encrypted_file({.write_page_index = true});
    const auto enc = key_info(/*with_aad_prefix=*/true);
    constexpr int kFrom = kNumRows - 10;
    ASSIGN_OR_ABORT(auto chunk, read_all(&enc, kFrom));
    ASSERT_EQ(10, chunk->num_rows());
    EXPECT_EQ(std::to_string(kFrom), chunk->get_column_by_index(0)->debug_item(0));
    EXPECT_EQ(std::to_string(kNumRows - 1), chunk->get_column_by_index(0)->debug_item(9));
    // Pages wholly below the bound were skipped via the decrypted page index rather than read.
    EXPECT_LT(_stats.raw_rows_read, kNumRows);
}

// Without the withheld AAD prefix the file must not open: the prefix binds the file to its identity in
// the table, so the footer fails authentication rather than decrypting under a guess.
TEST_F(EncryptedFileReaderTest, MissingWithheldAadPrefixFails) {
    write_encrypted_file({});
    const auto enc = key_info(/*with_aad_prefix=*/false);
    ASSERT_FALSE(read_all(&enc).ok());
}

TEST_F(EncryptedFileReaderTest, WrongKeyFails) {
    write_encrypted_file({});
    auto enc = key_info(/*with_aad_prefix=*/true);
    enc.__set_file_dek(std::string("fedcba9876543210"));
    auto result = read_all(&enc);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status().to_string();
}

// An encrypted file with no key from the planner is refused, never parsed as plaintext.
TEST_F(EncryptedFileReaderTest, NoPlannerKeyIsRefused) {
    write_encrypted_file({});
    auto result = read_all(/*enc_info=*/nullptr);
    ASSERT_FALSE(result.ok());
}

} // namespace starrocks::parquet
