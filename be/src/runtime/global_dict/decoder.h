// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "common/status.h"

namespace starrocks {

class GlobalDictDecoder {
public:
    virtual ~GlobalDictDecoder() = default;

    virtual Status decode(Column* in, Column* out) = 0;
};

using GlobalDictDecoderPtr = std::unique_ptr<GlobalDictDecoder>;

template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict);

} // namespace starrocks
