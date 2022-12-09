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

#include "common/statusor.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "storage/lake/tablet.h"
#include "storage/olap_common.h"

namespace starrocks::lake {

class SparkLoadHandler {
public:
    SparkLoadHandler() = default;
    ~SparkLoadHandler() = default;

    Status process_streaming_ingestion(Tablet& tablet, const TPushReq& request, PushType push_type,
                                       std::vector<TTabletInfo>* tablet_info_vec);

    int64_t write_bytes() const { return _write_bytes; }
    int64_t write_rows() const { return _write_rows; }

private:
    Status _load_convert(Tablet& cur_tablet);

    void _get_tablet_infos(const Tablet& tablet, std::vector<TTabletInfo>* tablet_info_vec);

private:
    // mainly tablet_id, version and delta file path
    TPushReq _request;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
};

} // namespace starrocks::lake
