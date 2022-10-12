// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

using TxnLog = TxnLogPB;
using TxnLogPtr = std::shared_ptr<const TxnLog>;
using MutableTxnLogPtr = std::shared_ptr<TxnLog>;

} // namespace starrocks::lake
