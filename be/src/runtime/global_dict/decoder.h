// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "common/status.h"

namespace starrocks {
namespace vectorized {

class GlobalDictDecoder {
public:
    virtual ~GlobalDictDecoder() = default;

    virtual Status decode(vectorized::Column* in, vectorized::Column* out) = 0;
};

using GlobalDictDecoderPtr = std::unique_ptr<GlobalDictDecoder>;

template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict);

} // namespace vectorized
} // namespace starrocks
