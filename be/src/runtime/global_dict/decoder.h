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

#pragma once

#include "column/column.h"
#include "common/status.h"

namespace starrocks {

class GlobalDictDecoder {
public:
    virtual ~GlobalDictDecoder() = default;

    virtual Status decode_string(const Column* in, Column* out) = 0;

    virtual Status decode_array(const Column* in, Column* out) = 0;
};

using GlobalDictDecoderPtr = std::unique_ptr<GlobalDictDecoder>;

<<<<<<< HEAD:be/src/runtime/global_dict/decoder.h
template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict);

} // namespace starrocks
=======
// Timeout (ms) for the Arrow Flight call to a Python UDF worker (local or external service_url).
// Applied as the gRPC deadline on the DoExchange stream, so a dead/unreachable/hung worker fails
// the query instead of hanging forever. Note it bounds the whole stream's lifetime, so set it above
// the longest expected UDF query. 0 (default) disables the timeout (wait indefinitely).
CONF_mInt32(python_udf_rpc_timeout_ms, "0");

} // namespace starrocks::config
>>>>>>> 7e0f4919b8 ([Enhancement] Add per-UDF service_url to connect an external Python UDF worker (#76466)):be/src/common/config_udf_fwd.h
