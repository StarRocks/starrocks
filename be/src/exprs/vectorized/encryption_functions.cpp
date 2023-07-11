// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/encryption_functions.h"

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/base64.h"
#include "exprs/expr.h"
#include "util/aes_util.h"
#include "util/debug_util.h"
#include "util/integer_util.h"
#include "util/md5.h"
#include "util/sha.h"

namespace starrocks::vectorized {

StatusOr<ColumnPtr> EncryptionFunctions::aes_encrypt(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row) || key_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        int cipher_len = src_value.size + 16;
        char p[cipher_len];

        auto key_value = key_viewer.value(row);
        int len = AesUtil::encrypt(AES_128_ECB, (unsigned char*)src_value.data, src_value.size,
                                   (unsigned char*)key_value.data, key_value.size, nullptr, true, (unsigned char*)p);
        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(p, len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> EncryptionFunctions::aes_decrypt(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (src_viewer.is_null(row) || key_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto src_value = src_viewer.value(row);
        auto key_value = key_viewer.value(row);
        if (src_value.size == 0 || key_value.size == 0) {
            result.append_null();
            continue;
        }

        int cipher_len = src_value.size;
        char p[cipher_len];

        int len = AesUtil::decrypt(AES_128_ECB, (unsigned char*)src_value.data, src_value.size,
                                   (unsigned char*)key_value.data, key_value.size, nullptr, true, (unsigned char*)p);

        if (len < 0) {
            result.append_null();
            continue;
        }

        result.append(Slice(p, len));
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
        if (src_value.size == 0) {
            result.append_null();
            continue;
        } else if (src_value.size > config::max_length_for_to_base64) {
            std::stringstream ss;
            ss << "to_base64 not supported length > " << config::max_length_for_to_base64;
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
        list.emplace_back(ColumnViewer<TYPE_VARCHAR>(col));
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
        list.emplace_back(ColumnViewer<TYPE_VARCHAR>(col));
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

} // namespace starrocks::vectorized
