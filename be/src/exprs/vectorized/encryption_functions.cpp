// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/encryption_functions.h"

#include <boost/smart_ptr.hpp>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/base64.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "util/aes_util.h"
#include "util/debug_util.h"
#include "util/md5.h"

namespace starrocks::vectorized {

ColumnPtr EncryptionFunctions::aes_encrypt(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result;
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

ColumnPtr EncryptionFunctions::aes_decrypt(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result;
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

ColumnPtr EncryptionFunctions::from_base64(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result;
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

ColumnPtr EncryptionFunctions::to_base64(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    const int size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result;
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

ColumnPtr EncryptionFunctions::md5sum(FunctionContext* ctx, const Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> list;
    list.reserve(columns.size());
    for (const ColumnPtr& col : columns) {
        list.emplace_back(ColumnViewer<TYPE_VARCHAR>(col));
    }

    ColumnBuilder<TYPE_VARCHAR> result;
    auto size = columns[0]->size();
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

ColumnPtr EncryptionFunctions::md5(FunctionContext* ctx, const Columns& columns) {
    auto src_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    ColumnBuilder<TYPE_VARCHAR> result;
    auto size = columns[0]->size();
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

} // namespace starrocks::vectorized
