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
        AesMode mode;
        const unsigned char* key_data;
        int key_len;
        const char* iv_data;
        int iv_len;
        const unsigned char* aad_data;
        uint32_t aad_len;
        bool is_valid;

        RowParameters()
                : mode(AES_128_ECB),
                  key_data(nullptr),
                  key_len(0),
                  iv_data(nullptr),
                  iv_len(0),
                  aad_data(nullptr),
                  aad_len(0),
                  is_valid(true) {}
    };

    // Constructor: initialize column viewers and cache constant parameters
    explicit AesParameterExtractor(const Columns& columns)
            : src_viewer_(columns[0]),
              key_viewer_(columns[1]),
              iv_viewer_(columns[2]),
              mode_viewer_(columns[3]),
              mode_is_const_(columns[3]->is_constant()),
              key_is_const_(columns[1]->is_constant()),
              iv_is_const_(columns[2]->is_constant()),
              mode_const_is_null_(false),
              key_const_is_null_(false),
              cached_mode_(AES_128_ECB),
              cached_key_data_(nullptr),
              cached_key_len_(0),
              cached_iv_data_(nullptr),
              cached_iv_len_(0) {
        // Check if 5th parameter (AAD for GCM mode) exists
        if (columns.size() >= 5) {
            aad_viewer_.emplace(columns[4]);
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
        if (iv_is_const_ && !iv_viewer_.is_null(0)) {
            auto iv_value = iv_viewer_.value(0);
            cached_iv_data_ = iv_value.data;
            cached_iv_len_ = iv_value.size;
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
        bool iv_is_null = iv_is_const_ ? (cached_iv_data_ == nullptr) : iv_viewer_.is_null(row);
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

        // Extract AAD (only for GCM mode)
        if (aad_viewer_.has_value() && !aad_viewer_->is_null(row)) {
            auto aad_value = aad_viewer_->value(row);
            params.aad_data = (const unsigned char*)aad_value.data;
            params.aad_len = aad_value.size;
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

    // Constant column null flags (true if constant column is NULL)
    bool mode_const_is_null_;
    bool key_const_is_null_;

    // Cached constant values
    AesMode cached_mode_;
    const unsigned char* cached_key_data_;
    int cached_key_len_;
    const char* cached_iv_data_;
    int cached_iv_len_;
};

// 4/5-parameter version: aes_encrypt(data, key, iv, mode, [aad])
// Parameters: data, key, iv, mode, [aad]
// - iv can be NULL for ECB mode
// - aad is optional and only used for GCM mode
StatusOr<ColumnPtr> EncryptionFunctions::aes_encrypt_with_mode(FunctionContext* ctx, const Columns& columns) {
    // Check only essential columns (data, key, mode) for only_null
    // IV and AAD are checked later based on the encryption mode
    CHECK_AES_ESSENTIAL_COLUMNS_NULL(columns);

    // Use parameter extractor to handle all parameter extraction and caching
    AesParameterExtractor extractor(columns);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

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

        // Use stack allocation (VLA - Variable Length Array)
        char encrypt_buf[cipher_len];

        int len = AesUtil::encrypt_ex(params.mode, (unsigned char*)src_value.data, src_value.size, params.key_data,
                                      params.key_len, params.iv_data, params.iv_len, true, (unsigned char*)encrypt_buf,
                                      params.aad_data, params.aad_len);

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(encrypt_buf, len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// 4/5-parameter version: aes_decrypt(data, key, iv, mode, [aad])
// Parameters: data, key, iv, mode, [aad]
// - iv can be NULL for ECB mode
// - aad is optional and only used for GCM mode
StatusOr<ColumnPtr> EncryptionFunctions::aes_decrypt_with_mode(FunctionContext* ctx, const Columns& columns) {
    // Check only essential columns (data, key, mode) for only_null
    // IV and AAD are checked later based on the encryption mode
    CHECK_AES_ESSENTIAL_COLUMNS_NULL(columns);

    // Use parameter extractor to handle all parameter extraction and caching
    AesParameterExtractor extractor(columns);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

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

        // Use stack allocation (VLA - Variable Length Array)
        char decrypt_buf[cipher_len];

        int len = AesUtil::decrypt_ex(params.mode, (unsigned char*)src_value.data, src_value.size, params.key_data,
                                      params.key_len, params.iv_data, params.iv_len, true, (unsigned char*)decrypt_buf,
                                      params.aad_data, params.aad_len);

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(decrypt_buf, len));
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

} // namespace starrocks

#include "gen_cpp/opcode/EncryptionFunctions.inc"
