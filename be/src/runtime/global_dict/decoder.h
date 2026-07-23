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
<<<<<<< HEAD:be/src/runtime/global_dict/decoder.h
=======
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/Types_types.h"
>>>>>>> 120992379a ([BugFix] Set query_delivery_timeout for external scan plans to bound the QueryContext second-chance tail (#76536)):be/src/orchestration/query_orchestrator.h

namespace starrocks {

class GlobalDictDecoder {
public:
    virtual ~GlobalDictDecoder() = default;

    virtual Status decode_string(const Column* in, Column* out) = 0;

<<<<<<< HEAD:be/src/runtime/global_dict/decoder.h
    virtual Status decode_array(const Column* in, Column* out) = 0;
=======
    // Build the TQueryOptions used to execute an external scan plan fragment (open_scanner).
    // Exposed as a static helper so tests can assert the fabricated options.
    static TQueryOptions build_external_query_options(const TScanOpenParams& params);

private:
    ExecEnv* _exec_env;
>>>>>>> 120992379a ([BugFix] Set query_delivery_timeout for external scan plans to bound the QueryContext second-chance tail (#76536)):be/src/orchestration/query_orchestrator.h
};

using GlobalDictDecoderPtr = std::unique_ptr<GlobalDictDecoder>;

template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict);

} // namespace starrocks
