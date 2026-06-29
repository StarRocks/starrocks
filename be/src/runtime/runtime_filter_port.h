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

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "column/column.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

struct TypeDescriptor;
class RuntimeFilter;
class RuntimeFilterBuildDescriptor;
class RuntimeState;

// RuntimeFilterPort is bind to a fragment instance
// and it's to exchange RF(publish/receive) with outside world.
class RuntimeFilterPort {
public:
    RuntimeFilterPort(RuntimeState* state) : _state(state) {}
    void publish_runtime_filters(const std::list<RuntimeFilterBuildDescriptor*>& rf_descs);

    void publish_runtime_filters_for_skew_broadcast_join(const std::list<RuntimeFilterBuildDescriptor*>& rf_descs,
                                                         const std::vector<Columns>& keyColumns,
                                                         const std::vector<bool>& null_safe,
                                                         const std::vector<TypeDescriptor>& type_descs);

    void publish_local_colocate_filters(std::list<RuntimeFilterBuildDescriptor*>& rf_descs);
    // receiver runtime filter allocated in this fragment instance(broadcast join generate it)
    // or allocated in this query(shuffle join generate global runtime filter)
    void receive_runtime_filter(int32_t filter_id, const RuntimeFilter* rf);
    void receive_shared_runtime_filter(int32_t filter_id, const std::shared_ptr<const RuntimeFilter>& rf);
    std::string listeners(int32_t filter_id);

private:
    void publish_skew_broadcast_join_key_columns(RuntimeFilterBuildDescriptor* rf_desc, const ColumnPtr& keyColumn,
                                                 bool null_safe, const TypeDescriptor& type_desc);
    void static prepare_params(PTransmitRuntimeFilterParams& params, RuntimeState* state,
                               RuntimeFilterBuildDescriptor* rf_desc);
    RuntimeState* _state;
};

} // namespace starrocks
