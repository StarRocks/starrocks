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
#include "common/util/thrift_util.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/string_input_stream.h"

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

// The plaintext FileCryptoMetaData parser. This is the one part of an encrypted file that sits
// OUTSIDE the AEAD, so a corrupt or hostile footer reaches it with nothing authenticated -- and its
// bounds are all that stand between a bad file and the scan thread. The parser is hand-rolled because
// arrow's generated FileCryptoMetaData::Make segfaults in this binary (see the note at its
// definition), which makes testing the bounds directly the only way to know they hold.
class ParseFileCryptoMetadataTest : public testing::Test {
protected:
    // Minimal compact-thrift writer. Field header is (delta << 4) | type; 8 = binary,
    // 12 = struct, 1 = BOOLEAN_TRUE, 9 = list; 0x00 ends a struct.
    struct Builder {
        std::vector<uint8_t> bytes;

        Builder& field(int delta, int type) {
            bytes.push_back(static_cast<uint8_t>((delta << 4) | type));
            return *this;
        }
        Builder& varint(uint64_t v) {
            while (v >= 0x80) {
                bytes.push_back(static_cast<uint8_t>((v & 0x7f) | 0x80));
                v >>= 7;
            }
            bytes.push_back(static_cast<uint8_t>(v));
            return *this;
        }
        Builder& binary(int delta, const std::string& s) {
            field(delta, 8).varint(s.size());
            bytes.insert(bytes.end(), s.begin(), s.end());
            return *this;
        }
        Builder& stop() {
            bytes.push_back(0x00);
            return *this;
        }
    };

    // FileCryptoMetaData { 1: EncryptionAlgorithm; 2: binary key_metadata }
    // EncryptionAlgorithm union { 1: AesGcmV1 | 2: AesGcmCtrV1 }
    // AesGcm* { 1: binary aad_prefix; 2: binary aad_file_unique; 3: bool supply_aad_prefix }
    static std::vector<uint8_t> valid_crypto_metadata(int algorithm_field, const std::string& aad_prefix,
                                                      const std::string& aad_file_unique, bool supply_aad_prefix,
                                                      const std::string& key_metadata) {
        Builder b;
        b.field(1, 12);               // encryption_algorithm union
        b.field(algorithm_field, 12); // AesGcmV1 (1) or AesGcmCtrV1 (2)
        if (!aad_prefix.empty()) {
            b.binary(1, aad_prefix);
            b.binary(1, aad_file_unique);
        } else {
            b.binary(2, aad_file_unique);
        }
        b.field(1, supply_aad_prefix ? 1 : 2); // bool is encoded in the type nibble
        b.stop();                              // end AesGcm*
        b.stop();                              // end union
        b.binary(1, key_metadata);
        b.stop(); // end FileCryptoMetaData
        return b.bytes;
    }
};

TEST_F(ParseFileCryptoMetadataTest, ParsesAesGcmV1) {
    const auto bytes = valid_crypto_metadata(1, "prefix", "unique-token", true, "key-metadata");
    ParsedCryptoMetadata out;
    ASSERT_OK(parse_file_crypto_metadata(bytes.data(), bytes.size(), &out));
    ASSERT_EQ(static_cast<int32_t>(::parquet::ParquetCipher::AES_GCM_V1), out.algorithm);
    ASSERT_EQ("prefix", out.aad_prefix);
    ASSERT_EQ("unique-token", out.aad_file_unique);
    ASSERT_TRUE(out.supply_aad_prefix);
    ASSERT_EQ("key-metadata", out.key_metadata);
    ASSERT_EQ(bytes.size(), out.consumed);
}

TEST_F(ParseFileCryptoMetadataTest, ParsesAesGcmCtrV1) {
    const auto bytes = valid_crypto_metadata(2, "", "unique", false, "km");
    ParsedCryptoMetadata out;
    ASSERT_OK(parse_file_crypto_metadata(bytes.data(), bytes.size(), &out));
    ASSERT_EQ(static_cast<int32_t>(::parquet::ParquetCipher::AES_GCM_CTR_V1), out.algorithm);
    ASSERT_TRUE(out.aad_prefix.empty());
    ASSERT_EQ("unique", out.aad_file_unique);
    ASSERT_FALSE(out.supply_aad_prefix);
}

// The crash vector: skip(type=12) and read_struct() are mutually recursive at ONE input byte per
// level, so without a depth bound a footer nests as deeply as it has bytes and exhausts the stack --
// an uncatchable SIGSEGV on the scan thread, needing no key to trigger.
TEST_F(ParseFileCryptoMetadataTest, DeeplyNestedStructsAreRefusedInsteadOfExhaustingTheStack) {
    std::vector<uint8_t> bytes(4096, 0x1C); // field delta 1, type 12 (struct), repeated
    ParsedCryptoMetadata out;
    Status st = parse_file_crypto_metadata(bytes.data(), bytes.size(), &out);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// The other unbounded loop: a list element count is not compared against the bytes remaining, and the
// bool element types consume NOTHING, so they never reach a bounds check. Six bytes could otherwise
// spin the loop ~2^32 times and wedge the thread.
TEST_F(ParseFileCryptoMetadataTest, OversizedListCountIsRefused) {
    Builder b;
    b.field(1, 9);           // a list field
    b.bytes.push_back(0xF1); // size nibble 0xF -> read varint; element type 1 (bool, consumes nothing)
    b.varint(0xFFFFFFFFULL); // a count no input could honestly contain
    b.stop();
    ParsedCryptoMetadata out;
    Status st = parse_file_crypto_metadata(b.bytes.data(), b.bytes.size(), &out);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(ParseFileCryptoMetadataTest, TruncatedInputIsRefused) {
    // Opens a struct and then runs out of bytes.
    const std::vector<uint8_t> bytes{0x1C};
    ParsedCryptoMetadata out;
    Status st = parse_file_crypto_metadata(bytes.data(), bytes.size(), &out);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(ParseFileCryptoMetadataTest, BinaryLengthPastTheEndIsRefused) {
    Builder b;
    b.field(1, 8).varint(1000); // claims 1000 bytes of payload that are not there
    ParsedCryptoMetadata out;
    Status st = parse_file_crypto_metadata(b.bytes.data(), b.bytes.size(), &out);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(ParseFileCryptoMetadataTest, UnhandledFieldTypeIsRefused) {
    Builder b;
    b.field(1, 15); // not a type the parser handles
    b.stop();
    ParsedCryptoMetadata out;
    Status st = parse_file_crypto_metadata(b.bytes.data(), b.bytes.size(), &out);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// Fields the parser does not care about must be skipped, not tripped over: every scalar type the
// writer may emit has to be walked correctly to reach the ones that matter.
TEST_F(ParseFileCryptoMetadataTest, UnknownScalarFieldsAreSkipped) {
    Builder b;
    b.field(1, 3).varint(7);     // i8 at field 1
    b.field(1, 5).varint(12345); // i32 at field 2
    b.field(1, 6).varint(99);    // i64 at field 3
    b.field(1, 7);               // double at field 4
    for (int i = 0; i < 8; i++) {
        b.bytes.push_back(0x00);
    }
    b.binary(1, "ignored"); // binary at field 5
    b.stop();
    ParsedCryptoMetadata out;
    // No algorithm or key metadata present, but nothing should throw or overrun either.
    ASSERT_OK(parse_file_crypto_metadata(b.bytes.data(), b.bytes.size(), &out));
    ASSERT_TRUE(out.key_metadata.empty());
}

// The PME bloom-filter reader. StarRocks' own writer emits no bloom filters, so this path is only
// reachable when reading a file another engine wrote -- a writer round trip cannot exercise it at all,
// which is why the logic was lifted out of RawColumnReader into a free function. The two length
// prefixes it reads sit OUTSIDE the AEAD, so the bounds on them are the interesting part.
class ReadEncryptedBloomFilterTest : public DecryptMetadataModuleTest {
protected:
    // One module as it appears on disk: [4-byte ciphertext length][nonce][ciphertext][tag].
    // encrypt_module() already produces exactly that framing.
    std::string module_bytes(const std::string& plaintext, int8_t module_type) const {
        const auto cipher = encrypt_module(plaintext, module_type, kRowGroup, kColumn);
        return std::string(reinterpret_cast<const char*>(cipher.data()), cipher.size());
    }

    // A serialized BloomFilterHeader declaring numBytes, using thrift so the reader's own
    // deserializer accepts it. algorithm/hash/compression are required unions, so each gets its one
    // member set -- exactly what parquet-mr and arrow emit.
    static std::string serialized_header(int32_t num_bytes) {
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());

        tparquet::BloomFilterHeader header;
        header.__set_numBytes(num_bytes);
        header.__set_algorithm(algorithm);
        header.__set_hash(hash);
        header.__set_compression(compression);
        ThriftSerializer ser(true, 256);
        uint32_t len = 0;
        uint8_t* buf = nullptr;
        CHECK(ser.serialize(&header, &len, &buf).ok());
        return std::string(reinterpret_cast<const char*>(buf), len);
    }

    Status read_from(const std::string& file_bytes, BloomFilter* bf) const {
        std::string copy = file_bytes;
        RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(copy)), "bloom-file");
        const FileMetaData meta = encrypted_metadata();
        const TParquetEncryptionInfo enc = scan_range_key();
        return read_encrypted_bloom_filter(&file, 0, meta, &enc, kRowGroup, kColumn, /*has_null_byte=*/0, bf);
    }
};

TEST_F(ReadEncryptedBloomFilterTest, HeaderAndBitsetModulesAreDecrypted) {
    // 32 is BlockSplitBloomFilter's minimum block size, so it is a bitset the filter will accept.
    constexpr int32_t kNumBytes = 32;
    const std::string bitset(kNumBytes, '\0');
    const std::string file = module_bytes(serialized_header(kNumBytes), kPmeModuleBloomFilterHeader) +
                             module_bytes(bitset, kPmeModuleBloomFilterBitset);

    ParquetBlockSplitBloomFilter bf;
    ASSERT_OK(read_from(file, &bf));
}

// The bitset length the header declares is authenticated; the decrypted bitset must match it, or the
// two modules do not belong together.
TEST_F(ReadEncryptedBloomFilterTest, BitsetLengthDisagreeingWithTheHeaderIsRefused) {
    const std::string file = module_bytes(serialized_header(32), kPmeModuleBloomFilterHeader) +
                             module_bytes(std::string(64, '\0'), kPmeModuleBloomFilterBitset);

    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(file, &bf);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("declares") != std::string::npos) << st.to_string();
}

TEST_F(ReadEncryptedBloomFilterTest, NonPositiveNumBytesIsRefused) {
    const std::string file = module_bytes(serialized_header(0), kPmeModuleBloomFilterHeader) +
                             module_bytes(std::string(32, '\0'), kPmeModuleBloomFilterBitset);

    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(file, &bf);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("numBytes") != std::string::npos) << st.to_string();
}

// The length prefix is unauthenticated, so a zero must be refused before it is used to size anything.
TEST_F(ReadEncryptedBloomFilterTest, ZeroModuleLengthIsRefused) {
    std::string file(64, '\0'); // a 4-byte prefix of zero, then padding
    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(file, &bf);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("bloom-filter module length") != std::string::npos) << st.to_string();
}

// Likewise an absurd one: bounded before allocating, not discovered by the read running off the end.
TEST_F(ReadEncryptedBloomFilterTest, OversizedModuleLengthIsRefused) {
    std::string file(64, '\0');
    // 0x00200000 == 2 MiB, over the 1 MiB cap on a header module.
    file[0] = '\x00';
    file[1] = '\x00';
    file[2] = '\x20';
    file[3] = '\x00';
    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(file, &bf);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_TRUE(st.to_string().find("bloom-filter module length") != std::string::npos) << st.to_string();
}

// A file that ends inside the module it advertises.
TEST_F(ReadEncryptedBloomFilterTest, TruncatedModuleIsRefused) {
    std::string full = module_bytes(serialized_header(32), kPmeModuleBloomFilterHeader);
    const std::string truncated = full.substr(0, full.size() / 2);
    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(truncated, &bf);
    ASSERT_FALSE(st.ok()) << st.to_string();
}

// The header module must be decrypted under kBloomFilterHeader. Sealing it as the bitset module type
// instead has to fail authentication rather than parse as a header.
TEST_F(ReadEncryptedBloomFilterTest, HeaderSealedUnderTheWrongModuleTypeFails) {
    const std::string file = module_bytes(serialized_header(32), kPmeModuleBloomFilterBitset) +
                             module_bytes(std::string(32, '\0'), kPmeModuleBloomFilterBitset);

    ParquetBlockSplitBloomFilter bf;
    Status st = read_from(file, &bf);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

} // namespace starrocks::parquet
