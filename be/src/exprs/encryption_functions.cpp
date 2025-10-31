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

#include "exprs/encryption_functions.h"

#include <optional>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/base64.h"
#include "types/logical_type_infra.h"
#include "util/aes_util.h"
#include "util/md5.h"
#include "util/sha.h"

namespace starrocks {

// Macro to check if essential columns (data, key, mode) are only_null
// Returns the first only_null column if any, otherwise continues execution
// Usage: CHECK_AES_ESSENTIAL_COLUMNS_NULL(columns)
#define CHECK_AES_ESSENTIAL_COLUMNS_NULL(cols)                                                            \
    do {                                                                                                  \
        if ((cols)[0]->only_null() || (cols)[1]->only_null() || (cols)[3]->only_null()) {                 \
            return (cols)[0]->only_null() ? (cols)[0] : ((cols)[1]->only_null() ? (cols)[1] : (cols)[3]); \
        }                                                                                                 \
    } while (0)

// Helper class to extract and cache AES parameters from columns
// This eliminates code duplication between encrypt and decrypt functions
class AesParameterExtractor {
public:
    // Structure to hold extracted parameters for a specific row
    struct RowParameters {
        AesMode mode{AES_128_ECB};
        const unsigned char* key_data{nullptr};
        uint32_t key_len{0};
        const char* iv_data{nullptr};
        uint32_t iv_len{0};
        const unsigned char* aad_data{nullptr};
        uint32_t aad_len{0};
        bool is_valid{true};

        RowParameters() = default;
    };

    // Constructor: initialize column viewers and cache constant parameters
    explicit AesParameterExtractor(const Columns& columns)
            : src_viewer_(columns[0]),
              key_viewer_(columns[1]),
              iv_viewer_(columns[2]),
              mode_viewer_(columns[3]),
              mode_is_const_(columns[3]->is_constant()),
              key_is_const_(columns[1]->is_constant()),
              iv_is_const_(columns[2]->is_constant()) {
        // Check if 5th parameter (AAD for GCM mode) exists
        if (columns.size() >= 5) {
            aad_viewer_.emplace(columns[4]);
            aad_is_const_ = columns[4]->is_constant();
        }

        // Cache constant mode
        if (mode_is_const_) {
            if (mode_viewer_.is_null(0)) {
                mode_const_is_null_ = true;
            } else {
                auto mode_value = mode_viewer_.value(0);
                std::string mode_str(mode_value.data, mode_value.size);
                cached_mode_ = AesUtil::get_mode_from_string(mode_str);
            }
        }

        // Cache constant key
        if (key_is_const_) {
            if (key_viewer_.is_null(0)) {
                key_const_is_null_ = true;
            } else {
                auto key_value = key_viewer_.value(0);
                cached_key_data_ = (const unsigned char*)key_value.data;
                cached_key_len_ = key_value.size;
            }
        }

        // Cache constant iv
        if (iv_is_const_) {
            if (iv_viewer_.is_null(0)) {
                iv_const_is_null_ = true;
            } else {
                auto iv_value = iv_viewer_.value(0);
                cached_iv_data_ = iv_value.data;
                cached_iv_len_ = iv_value.size;
            }
        }

        // Cache constant aad
        if (aad_is_const_ && aad_viewer_.has_value()) {
            if (aad_viewer_->is_null(0)) {
                aad_const_is_null_ = true;
            } else {
                auto aad_value = aad_viewer_->value(0);
                cached_aad_data_ = (const unsigned char*)aad_value.data;
                cached_aad_len_ = aad_value.size;
            }
        }
    }

    // Extract parameters for a specific row
    RowParameters extract(int row) {
        RowParameters params;

        // Check for null values in required columns (src, key, mode are always required)
        // For constant columns, use cached null flag; for non-constant columns, check each row
        bool src_is_null = src_viewer_.is_null(row);
        bool key_is_null = key_is_const_ ? key_const_is_null_ : key_viewer_.is_null(row);
        bool mode_is_null = mode_is_const_ ? mode_const_is_null_ : mode_viewer_.is_null(row);

        if (src_is_null || key_is_null || mode_is_null) {
            params.is_valid = false;
            return params;
        }

        // Extract mode first (use cached value if constant)
        if (mode_is_const_) {
            params.mode = cached_mode_;
        } else {
            auto mode_value = mode_viewer_.value(row);
            std::string mode_str(mode_value.data, mode_value.size);
            params.mode = AesUtil::get_mode_from_string(mode_str);
        }

        // Check if IV is required for this mode
        // ECB mode does not require IV, other modes do
        bool iv_is_null = iv_is_const_ ? iv_const_is_null_ : iv_viewer_.is_null(row);
        if (!AesUtil::is_ecb_mode(params.mode) && iv_is_null) {
            // Non-ECB mode requires IV, but IV is NULL
            params.is_valid = false;
            return params;
        }

        // Extract key (use cached value if constant)
        if (key_is_const_) {
            params.key_data = cached_key_data_;
            params.key_len = cached_key_len_;
        } else {
            auto key_value = key_viewer_.value(row);
            params.key_data = (const unsigned char*)key_value.data;
            params.key_len = key_value.size;
        }

        // Extract iv (use cached value if constant)
        if (iv_is_const_) {
            params.iv_data = cached_iv_data_;
            params.iv_len = cached_iv_len_;
        } else {
            if (!iv_viewer_.is_null(row)) {
                auto iv_value = iv_viewer_.value(row);
                params.iv_data = iv_value.data;
                params.iv_len = iv_value.size;
            }
        }

        // Extract AAD (only for GCM mode, use cached value if constant)
        if (aad_viewer_.has_value()) {
            bool aad_is_null = aad_is_const_ ? aad_const_is_null_ : aad_viewer_->is_null(row);
            if (!aad_is_null) {
                if (aad_is_const_) {
                    params.aad_data = cached_aad_data_;
                    params.aad_len = cached_aad_len_;
                } else {
                    auto aad_value = aad_viewer_->value(row);
                    params.aad_data = (const unsigned char*)aad_value.data;
                    params.aad_len = aad_value.size;
                }
            }
        }

        return params;
    }

    // Get source data viewer (for accessing source data in main loop)
    const ColumnViewer<TYPE_VARCHAR>& src_viewer() const { return src_viewer_; }

private:
    ColumnViewer<TYPE_VARCHAR> src_viewer_;
    ColumnViewer<TYPE_VARCHAR> key_viewer_;
    ColumnViewer<TYPE_VARCHAR> iv_viewer_;
    ColumnViewer<TYPE_VARCHAR> mode_viewer_;
    std::optional<ColumnViewer<TYPE_VARCHAR>> aad_viewer_;

    // Constant column flags
    bool mode_is_const_;
    bool key_is_const_;
    bool iv_is_const_;
    bool aad_is_const_ = false;

    // Constant column null flags (true if constant column is NULL)
    bool mode_const_is_null_ = false;
    bool key_const_is_null_ = false;
    bool iv_const_is_null_ = false;
    bool aad_const_is_null_ = false;

    // Cached constant values
    AesMode cached_mode_{AES_128_ECB};
    const unsigned char* cached_key_data_{nullptr};
    uint32_t cached_key_len_{0};
    const char* cached_iv_data_{nullptr};
    uint32_t cached_iv_len_{0};
    const unsigned char* cached_aad_data_{nullptr};
    uint32_t cached_aad_len_{0};
};

// Helper function to handle 2-parameter AES encryption: aes_encrypt(data, key)
// Uses default ECB mode with NULL IV for backward compatibility
static StatusOr<ColumnPtr> aes_encrypt_2params(FunctionContext* ctx, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    // Check if data or key columns are only_null
    if (columns[0]->only_null() || columns[1]->only_null()) {
        return columns[0]->only_null() ? columns[0] : columns[1];
    }

    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    // Reuse buffer across all rows
    std::vector<unsigned char> encrypt_buf;

    // Use default mode: AES_128_ECB
    const AesMode default_mode = AES_128_ECB;

    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row) || key_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        auto key_value = key_viewer.value(row);

        // ECB mode: may need padding (max one block = 16 bytes)
        int cipher_len = src_value.size + 16;

        if (encrypt_buf.size() < static_cast<size_t>(cipher_len)) {
            encrypt_buf.resize(cipher_len);
        }

        // ECB mode doesn't use IV, so pass nullptr for iv_data
        int len = AesUtil::encrypt_ex(default_mode, (unsigned char*)src_value.data, src_value.size,
                                      (const unsigned char*)key_value.data, key_value.size, nullptr,
                                      0,                                     // No IV for ECB
                                      true, encrypt_buf.data(), nullptr, 0); // No AAD

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(reinterpret_cast<char*>(encrypt_buf.data()), len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// Helper function to handle 2-parameter AES decryption: aes_decrypt(data, key)
// Uses default ECB mode with NULL IV for backward compatibility
static StatusOr<ColumnPtr> aes_decrypt_2params(FunctionContext* ctx, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    // Check if data or key columns are only_null
    if (columns[0]->only_null() || columns[1]->only_null()) {
        return columns[0]->only_null() ? columns[0] : columns[1];
    }

    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    // Reuse buffer across all rows
    std::vector<unsigned char> decrypt_buf;

    // Use default mode: AES_128_ECB
    const AesMode default_mode = AES_128_ECB;

    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row) || key_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        auto key_value = key_viewer.value(row);

        // Additional validation for decryption
        if (src_value.size == 0 || key_value.size == 0) {
            result.append_null();
            continue;
        }

        // Decrypted plaintext will never exceed ciphertext size
        int cipher_len = src_value.size;

        if (decrypt_buf.size() < static_cast<size_t>(cipher_len)) {
            decrypt_buf.resize(cipher_len);
        }

        // ECB mode doesn't use IV, so pass nullptr for iv_data
        int len = AesUtil::decrypt_ex(default_mode, (unsigned char*)src_value.data, src_value.size,
                                      (const unsigned char*)key_value.data, key_value.size, nullptr,
                                      0,                                     // No IV for ECB
                                      true, decrypt_buf.data(), nullptr, 0); // No AAD

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(reinterpret_cast<char*>(decrypt_buf.data()), len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// 2/4/5-parameter version: aes_encrypt(data, key, [iv, mode, [aad]])
// Parameters: data, key, [iv, mode, [aad]]
// - 2-parameter version uses default ECB mode with NULL IV (for backward compatibility)
// - iv can be NULL for ECB mode
// - aad is optional and only used for GCM mode
StatusOr<ColumnPtr> EncryptionFunctions::aes_encrypt_with_mode(FunctionContext* ctx, const Columns& columns) {
    // Handle 2-parameter version: aes_encrypt(data, key)
    // Use default mode (AES_128_ECB) and NULL IV
    if (columns.size() == 2) {
        return aes_encrypt_2params(ctx, columns);
    }

    // Check only essential columns (data, key, mode) for only_null
    // IV and AAD are checked later based on the encryption mode
    CHECK_AES_ESSENTIAL_COLUMNS_NULL(columns);

    // Use parameter extractor to handle all parameter extraction and caching
    AesParameterExtractor extractor(columns);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    // Reuse buffer across all rows to reduce memory allocation overhead
    std::vector<unsigned char> encrypt_buf;

    for (int row = 0; row < size; ++row) {
        // Extract parameters for current row
        auto params = extractor.extract(row);
        if (!params.is_valid) {
            result.append_null();
            continue;
        }

        auto src_value = extractor.src_viewer().value(row);

        // Calculate output buffer size precisely
        // GCM mode: IV (12 bytes) + plaintext + tag (16 bytes)
        // Stream modes (CTR/CFB/OFB): no padding, same as plaintext
        // Block modes (ECB/CBC): may need one block padding (16 bytes max)
        int cipher_len;
        if (AesUtil::is_gcm_mode(params.mode)) {
            // GCM output format: [IV][Ciphertext][TAG]
            cipher_len = src_value.size + 12 + AesUtil::GCM_TAG_SIZE;
        } else if (params.mode >= AES_128_CFB && params.mode <= AES_256_OFB) {
            // Stream modes: CFB, OFB (no padding needed)
            cipher_len = src_value.size;
        } else if (params.mode >= AES_128_CTR && params.mode <= AES_256_CTR) {
            // CTR mode (no padding needed)
            cipher_len = src_value.size;
        } else {
            // Block modes: ECB, CBC (may need padding, max one block = 16 bytes)
            cipher_len = src_value.size + 16;
        }

        // Resize buffer only if needed (vector will grow but not shrink)
        if (encrypt_buf.size() < static_cast<size_t>(cipher_len)) {
            encrypt_buf.resize(cipher_len);
        }

        int len = AesUtil::encrypt_ex(params.mode, (unsigned char*)src_value.data, src_value.size, params.key_data,
                                      params.key_len, params.iv_data, params.iv_len, true, encrypt_buf.data(),
                                      params.aad_data, params.aad_len);

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(reinterpret_cast<char*>(encrypt_buf.data()), len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// 2/4/5-parameter version: aes_decrypt(data, key, [iv, mode, [aad]])
// Parameters: data, key, [iv, mode, [aad]]
// - 2-parameter version uses default ECB mode with NULL IV (for backward compatibility)
// - iv can be NULL for ECB mode
// - aad is optional and only used for GCM mode
StatusOr<ColumnPtr> EncryptionFunctions::aes_decrypt_with_mode(FunctionContext* ctx, const Columns& columns) {
    // Handle 2-parameter version: aes_decrypt(data, key)
    // Use default mode (AES_128_ECB) and NULL IV
    if (columns.size() == 2) {
        return aes_decrypt_2params(ctx, columns);
    }

    // Check only essential columns (data, key, mode) for only_null
    // IV and AAD are checked later based on the encryption mode
    CHECK_AES_ESSENTIAL_COLUMNS_NULL(columns);

    // Use parameter extractor to handle all parameter extraction and caching
    AesParameterExtractor extractor(columns);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    // Reuse buffer across all rows to reduce memory allocation overhead
    std::vector<unsigned char> decrypt_buf;

    for (int row = 0; row < size; ++row) {
        // Extract parameters for current row
        auto params = extractor.extract(row);
        if (!params.is_valid) {
            result.append_null();
            continue;
        }

        auto src_value = extractor.src_viewer().value(row);

        // Additional validation for decryption
        if (src_value.size == 0 || params.key_len == 0) {
            result.append_null();
            continue;
        }

        // Calculate output buffer size for decryption
        // Decrypted plaintext will never exceed ciphertext size
        int cipher_len = src_value.size;

        // Resize buffer only if needed (vector will grow but not shrink)
        if (decrypt_buf.size() < static_cast<size_t>(cipher_len)) {
            decrypt_buf.resize(cipher_len);
        }

        int len = AesUtil::decrypt_ex(params.mode, (unsigned char*)src_value.data, src_value.size, params.key_data,
                                      params.key_len, params.iv_data, params.iv_len, true, decrypt_buf.data(),
                                      params.aad_data, params.aad_len);

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(reinterpret_cast<char*>(decrypt_buf.data()), len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::from_base64(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        if (src_value.size == 0) {
            result.append_null();
            continue;
        }

        int cipher_len = src_value.size;
        std::unique_ptr<char[]> p;
        p.reset(new char[cipher_len + 3]);

        int len = base64_decode2(src_value.data, src_value.size, p.get());
        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(p.get(), len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::to_base64(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        auto limit = config::max_length_for_to_base64;
        if (src_value.size == 0) {
            result.append_null();
            continue;
        } else if (src_value.size > limit) {
            std::stringstream ss;
            ss << "to_base64 not supported length > " << limit;
            throw std::runtime_error(ss.str());
        }

        int cipher_len = (size_t)(4.0 * ceil((double)src_value.size / 3.0)) + 1;
        char p[cipher_len];

        int len = base64_encode2((unsigned char*)src_value.data, src_value.size, (unsigned char*)p);
        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(p, len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::md5sum(FunctionContext* ctx, const Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> list;
    list.reserve(columns.size());
    for (const ColumnPtr& col : columns) {
        list.emplace_back(col);
    }

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        Md5Digest digest;
        for (auto& view : list) {
            if (view.is_null(row)) {
                continue;
            }
            auto v = view.value(row);
            digest.update(v.data, v.size);
        }
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::md5sum_numeric(FunctionContext* ctx, const Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> list;
    list.reserve(columns.size());
    for (const ColumnPtr& col : columns) {
        list.emplace_back(col);
    }
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_LARGEINT> result(size);
    for (int row = 0; row < size; row++) {
        Md5Digest digest;
        for (auto& view : list) {
            if (view.is_null(row)) {
                continue;
            }
            auto v = view.value(row);
            digest.update(v.data, v.size);
        }
        digest.digest();
        StringParser::ParseResult parse_res;
        auto int_val =
                StringParser::string_to_int<uint128_t>(digest.hex().c_str(), digest.hex().size(), 16, &parse_res);
        DCHECK_EQ(parse_res, StringParser::PARSE_SUCCESS);
        result.append(int_val);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::md5(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        Md5Digest digest;
        digest.update(src_value.data, src_value.size);
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

Status EncryptionFunctions::sha2_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    auto hash_length = ColumnHelper::get_const_value<TYPE_INT>(column);

    ScalarFunction function;
    if (hash_length == 224) {
        function = &EncryptionFunctions::sha224;
    } else if (hash_length == 256 || hash_length == 0) {
        function = &EncryptionFunctions::sha256;
    } else if (hash_length == 384) {
        function = &EncryptionFunctions::sha384;
    } else if (hash_length == 512) {
        function = &EncryptionFunctions::sha512;
    } else {
        function = EncryptionFunctions::invalid_sha;
    }

    auto fc = new EncryptionFunctions::SHA2Ctx();
    fc->function = function;
    context->set_function_state(scope, fc);
    return Status::OK();
}

StatusOr<ColumnPtr> EncryptionFunctions::invalid_sha(FunctionContext* ctx, const Columns& columns) {
    auto size = columns[0]->size();
    return ColumnHelper::create_const_null_column(size);
}

StatusOr<ColumnPtr> EncryptionFunctions::sha224(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        SHA224Digest digest;
        digest.update(src_value.data, src_value.size);
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::sha256(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        SHA256Digest digest;
        digest.update(src_value.data, src_value.size);
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::sha384(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        SHA384Digest digest;
        digest.update(src_value.data, src_value.size);
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::sha512(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; row++) {
        if (src_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        SHA512Digest digest;
        digest.update(src_value.data, src_value.size);
        digest.digest();

        result.append(Slice(digest.hex().c_str(), digest.hex().size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::sha2(FunctionContext* ctx, const Columns& columns) {
    if (!ctx->is_notnull_constant_column(1)) {
        auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
        auto length_viewer = ColumnViewer<TYPE_INT>(columns[1]);

        auto size = columns[0]->size();
        ColumnBuilder<TYPE_VARCHAR> result(size);

        for (int row = 0; row < size; row++) {
            if (src_viewer.is_null(row) || length_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            auto src_value = src_viewer.value(row);
            auto length = length_viewer.value(row);

            if (length == 224) {
                SHA224Digest digest;
                digest.update(src_value.data, src_value.size);
                digest.digest();
                result.append(Slice(digest.hex().c_str(), digest.hex().size()));
            } else if (length == 0 || length == 256) {
                SHA256Digest digest;
                digest.update(src_value.data, src_value.size);
                digest.digest();
                result.append(Slice(digest.hex().c_str(), digest.hex().size()));
            } else if (length == 384) {
                SHA384Digest digest;
                digest.update(src_value.data, src_value.size);
                digest.digest();
                result.append(Slice(digest.hex().c_str(), digest.hex().size()));
            } else if (length == 512) {
                SHA512Digest digest;
                digest.update(src_value.data, src_value.size);
                digest.digest();
                result.append(Slice(digest.hex().c_str(), digest.hex().size()));
            } else {
                result.append_null();
            }
        }

        return result.build(ColumnHelper::is_all_const(columns));
    }

    auto ctc = reinterpret_cast<SHA2Ctx*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return ctc->function(ctx, columns);
}

Status EncryptionFunctions::sha2_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto fc = reinterpret_cast<SHA2Ctx*>(context->get_function_state(scope));
        delete fc;
    }

    return Status::OK();
}

// Type markers to distinguish between different types in the hash digest
enum class RowFingerprintValueType : uint8_t {
    Null = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Int128 = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Decimal = 9,
    Date = 10,
    DateTime = 11,
};

// Template helper to encode a column into SHA256 digests based on its logical type
template <LogicalType LT>
struct EncodeColumnToDigest {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;

    static void encode(const Column* data_col, const NullColumn* null_col, size_t chunk_size,
                       std::vector<SHA256Digest>& digests) {
        if constexpr (lt_is_string<LT> || lt_is_binary<LT>) {
            // String/Binary types
            auto* col = down_cast<const ColumnType*>(data_col);
            const uint8_t* null_data = null_col ? null_col->raw_data() : nullptr;
            for (size_t row = 0; row < chunk_size; row++) {
                if (null_data && null_data[row]) {
                    uint8_t marker = static_cast<uint8_t>(RowFingerprintValueType::Null);
                    digests[row].update(&marker, 1);
                } else {
                    uint8_t marker = static_cast<uint8_t>(RowFingerprintValueType::String);
                    digests[row].update(&marker, 1);
                    Slice value = col->get_slice(row);
                    digests[row].update(value.data, value.size);
                }
            }
        } else if constexpr (lt_is_arithmetic<LT> || lt_is_date_or_datetime<LT> || lt_is_decimal<LT> ||
                             lt_is_largeint<LT>) {
            // Fixed-length types: numerics, dates, decimals
            auto* data = reinterpret_cast<const CppType*>(data_col->raw_data());
            RowFingerprintValueType marker_type;

            if constexpr (sizeof(CppType) == 1) {
                marker_type = RowFingerprintValueType::Int8;
            } else if constexpr (sizeof(CppType) == 2) {
                marker_type = RowFingerprintValueType::Int16;
            } else if constexpr (sizeof(CppType) == 4) {
                marker_type = (LT == TYPE_FLOAT) ? RowFingerprintValueType::Float
                              : lt_is_date<LT>   ? RowFingerprintValueType::Date
                                                 : RowFingerprintValueType::Int32;
            } else if constexpr (sizeof(CppType) == 8) {
                marker_type = (LT == TYPE_DOUBLE)  ? RowFingerprintValueType::Double
                              : lt_is_datetime<LT> ? RowFingerprintValueType::DateTime
                                                   : RowFingerprintValueType::Int64;
            } else if constexpr (sizeof(CppType) == 16) {
                marker_type = lt_is_decimal<LT> ? RowFingerprintValueType::Decimal : RowFingerprintValueType::Int128;
            } else {
                marker_type = RowFingerprintValueType::Decimal; // For 256-bit decimals
            }

            uint8_t marker = static_cast<uint8_t>(marker_type);
            const uint8_t* null_data = null_col ? null_col->raw_data() : nullptr;
            for (size_t row = 0; row < chunk_size; row++) {
                if (null_data && null_data[row]) {
                    uint8_t null_marker = static_cast<uint8_t>(RowFingerprintValueType::Null);
                    digests[row].update(&null_marker, 1);
                } else {
                    digests[row].update(&marker, 1);
                    digests[row].update(&data[row], sizeof(CppType));
                }
            }
        } else {
            // Fallback for unsupported types (JSON, HLL, OBJECT, STRUCT, ARRAY, MAP, etc.)
            // Cast to string representation and encode
            auto* col = down_cast<const ColumnType*>(data_col);
            const uint8_t* null_data = null_col ? null_col->raw_data() : nullptr;
            for (size_t row = 0; row < chunk_size; row++) {
                if (null_data && null_data[row]) {
                    uint8_t marker = static_cast<uint8_t>(RowFingerprintValueType::Null);
                    digests[row].update(&marker, 1);
                } else {
                    uint8_t marker = static_cast<uint8_t>(RowFingerprintValueType::String);
                    digests[row].update(&marker, 1);
                    // Convert the value to string and encode
                    std::string str_value = col->debug_item(row);
                    digests[row].update(str_value.data(), str_value.size());
                }
            }
        }
    }
};

StatusOr<ColumnPtr> EncryptionFunctions::encode_fingerprint_sha256(FunctionContext* ctx, const Columns& columns) {
    size_t chunk_size = columns[0]->size();
    std::vector<SHA256Digest> digests(chunk_size);

    // Process each column using template dispatch
    for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
        const ColumnPtr& col = columns[col_idx];
        const Column* data_col = ColumnHelper::get_data_column(col.get());
        const NullColumn* null_col =
                col->is_nullable() ? down_cast<const NullableColumn*>(col.get())->null_column().get() : nullptr;

        // Get logical type from FunctionContext
        const auto* type_desc = ctx->get_arg_type(col_idx);
        DCHECK(type_desc != nullptr) << "FunctionContext must have arg types initialized";
        LogicalType type = type_desc->type;

        // Use type dispatch to call the appropriate template specialization
        type_dispatch_filter(type, false, [&]<LogicalType LT>() -> bool {
            EncodeColumnToDigest<LT>::encode(data_col, null_col, chunk_size, digests);
            return true;
        });
    }

    // Build result column with raw binary digests as VARBINARY
    ColumnBuilder<TYPE_VARBINARY> result(chunk_size);
    for (size_t row = 0; row < chunk_size; row++) {
        digests[row].digest();
        // Use raw binary (32 bytes) instead of hex (64 bytes) - 50% more efficient
        result.append(Slice(reinterpret_cast<const char*>(digests[row].binary()), digests[row].binary_size()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/EncryptionFunctions.inc"
