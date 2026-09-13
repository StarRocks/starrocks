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
#include <parquet/encryption/encryption.h>
#include <parquet/encryption/encryption_internal.h>

#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "formats/parquet/metadata.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks::parquet {

// Parquet Modular Encryption gives every module its own AAD, derived from
// (module type, row group, column, page). The page index and bloom filters are separate modules from
// the pages, so getting any component of that derivation wrong does not fail loudly -- GCM simply
// refuses to authenticate, and a valid file reports as corrupt. These tests pin the derivation by
// encrypting with arrow directly, exactly as the writer does, and asserting the reader recovers it
// only when every AAD component matches.
class DecryptMetadataModuleTest : public testing::Test {
protected:
    static constexpr int32_t kKeyLen = 16;
    static constexpr int16_t kRowGroup = 3;
    static constexpr int16_t kColumn = 2;

    std::string dek() const { return std::string(kKeyLen, '\x5a'); }
    std::string aad_file_unique() const { return "aad-file-unique-token"; }
    std::string aad_prefix() const { return "prefix0123456789"; }

    FileMetaData encrypted_metadata() const {
        FileMetaData meta;
        FileMetaData::EncryptionContext ctx;
        ctx.aad_file_unique = aad_file_unique();
        ctx.aad_prefix = aad_prefix();
        ctx.algorithm = static_cast<int32_t>(::parquet::ParquetCipher::AES_GCM_V1);
        meta.set_encryption_ctx(std::move(ctx));
        return meta;
    }

    TParquetEncryptionInfo scan_range_key() const {
        TParquetEncryptionInfo enc;
        enc.__set_file_dek(dek());
        return enc;
    }

    // Produce the on-disk bytes for one module, the same way arrow's writer does: a metadata
    // encryptor (GCM even in a GCM_CTR file), length-prefixed, with the module AAD applied.
    std::vector<uint8_t> encrypt_module(const std::string& plaintext, int8_t module_type, int16_t row_group,
                                        int16_t column) const {
        const std::string file_aad = aad_prefix() + aad_file_unique();
        const std::string aad = ::parquet::encryption::CreateModuleAad(file_aad, module_type, row_group, column,
                                                                       ::parquet::kNonPageOrdinal);
        auto encryptor = ::parquet::encryption::AesEncryptor::Make(::parquet::ParquetCipher::AES_GCM_V1, kKeyLen,
                                                                   /*metadata=*/true);
        std::vector<uint8_t> cipher(encryptor->CiphertextLength(static_cast<int64_t>(plaintext.size())));
        const std::string key = dek();
        const int32_t written = encryptor->Encrypt(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(plaintext.data()), plaintext.size()),
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(aad.data()), aad.size()),
                std::span<uint8_t>(cipher.data(), cipher.size()));
        cipher.resize(static_cast<size_t>(written));
        return cipher;
    }
};

TEST_F(DecryptMetadataModuleTest, ColumnIndexModuleRoundTrips) {
    const std::string expected = "a serialized ColumnIndex would live here";
    const auto cipher = encrypt_module(expected, kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    ASSERT_OK(decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup, kColumn, cipher.data(),
                                      cipher.size(), &plaintext));
    ASSERT_EQ(expected, plaintext);
}

TEST_F(DecryptMetadataModuleTest, OffsetIndexModuleRoundTrips) {
    const std::string expected = "a serialized OffsetIndex would live here";
    const auto cipher = encrypt_module(expected, kPmeModuleOffsetIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    ASSERT_OK(decrypt_metadata_module(meta, &enc, kPmeModuleOffsetIndex, kRowGroup, kColumn, cipher.data(),
                                      cipher.size(), &plaintext));
    ASSERT_EQ(expected, plaintext);
}

TEST_F(DecryptMetadataModuleTest, BloomFilterModulesRoundTrip) {
    for (int8_t module : {kPmeModuleBloomFilterHeader, kPmeModuleBloomFilterBitset}) {
        const std::string expected = "bloom bytes";
        const auto cipher = encrypt_module(expected, module, kRowGroup, kColumn);
        const FileMetaData meta = encrypted_metadata();
        const TParquetEncryptionInfo enc = scan_range_key();

        std::string plaintext;
        ASSERT_OK(decrypt_metadata_module(meta, &enc, module, kRowGroup, kColumn, cipher.data(), cipher.size(),
                                          &plaintext));
        ASSERT_EQ(expected, plaintext) << "module=" << static_cast<int>(module);
    }
}

// The three assertions that prove the AAD is genuinely bound rather than incidental. Each of these
// would pass if the module AAD were ignored, which is exactly the mistake that is invisible on a
// happy-path round trip.
TEST_F(DecryptMetadataModuleTest, WrongModuleTypeFails) {
    const auto cipher = encrypt_module("payload", kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleOffsetIndex, kRowGroup, kColumn, cipher.data(),
                                        cipher.size(), &plaintext);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(DecryptMetadataModuleTest, WrongRowGroupOrdinalFails) {
    const auto cipher = encrypt_module("payload", kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup + 1, kColumn, cipher.data(),
                                        cipher.size(), &plaintext);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(DecryptMetadataModuleTest, WrongColumnOrdinalFails) {
    const auto cipher = encrypt_module("payload", kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup, kColumn + 1, cipher.data(),
                                        cipher.size(), &plaintext);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// A different key must not decrypt, which also demonstrates the key really comes from the scan range
// rather than anything cached on the FileMetaData.
TEST_F(DecryptMetadataModuleTest, WrongKeyFails) {
    const auto cipher = encrypt_module("payload", kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();
    TParquetEncryptionInfo enc;
    enc.__set_file_dek(std::string(kKeyLen, '\x11'));

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup, kColumn, cipher.data(),
                                        cipher.size(), &plaintext);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(DecryptMetadataModuleTest, MissingPlannerKeyIsRefused) {
    const auto cipher = encrypt_module("payload", kPmeModuleColumnIndex, kRowGroup, kColumn);
    const FileMetaData meta = encrypted_metadata();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, nullptr, kPmeModuleColumnIndex, kRowGroup, kColumn, cipher.data(),
                                        cipher.size(), &plaintext);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("no decryption key was provided by the planner") != std::string::npos)
            << st.to_string();
}

TEST_F(DecryptMetadataModuleTest, EmptyModuleIsRefused) {
    const FileMetaData meta = encrypted_metadata();
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup, kColumn, nullptr, 0, &plaintext);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(DecryptMetadataModuleTest, UnencryptedFileIsAProgrammingError) {
    FileMetaData meta; // no encryption context
    const TParquetEncryptionInfo enc = scan_range_key();

    std::string plaintext;
    Status st = decrypt_metadata_module(meta, &enc, kPmeModuleColumnIndex, kRowGroup, kColumn,
                                        reinterpret_cast<const uint8_t*>("x"), 1, &plaintext);
    ASSERT_TRUE(st.is_internal_error()) << st.to_string();
}

} // namespace starrocks::parquet
