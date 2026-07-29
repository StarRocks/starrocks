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

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

// Whether change data capture is on for this table. Duplicate-key and aggregate tables are always on:
// they derive their changes from the tablet metadata at no extra cost. Primary-key tables need extra
// per-publish metadata, so they opt in via enable_cdc to bound that cost on loads.
inline bool cdc_enabled(const TabletMetadataPB& metadata) {
    return metadata.schema().keys_type() != KeysType::PRIMARY_KEYS || metadata.cdc_metadata().enable_cdc();
}

// Apply an ALTER of the capture switch.
inline void alter_cdc(TabletMetadataPB* metadata, bool enable_cdc) {
    metadata->mutable_cdc_metadata()->set_enable_cdc(enable_cdc);
    if (!enable_cdc) {
        metadata->mutable_cdc_metadata()->clear_capture_status();
        metadata->mutable_cdc_metadata()->clear_pk_change_locator();
    }
}

// Initialize the CDC state at the start of a publish, assuming the metadata was copied from the previous publish.
inline void init_cdc(TabletMetadataPB* metadata) {
    if (!cdc_enabled(*metadata) || !metadata->has_cdc_metadata()) {
        return;
    }
    auto* cdc = metadata->mutable_cdc_metadata();
    cdc->clear_capture_status();
    cdc->clear_pk_change_locator();
}

// Record whether this publish's change can be captured.
inline void set_capture_status_if_cdc_enabled(TabletMetadataPB* metadata, const Status& status) {
    if (cdc_enabled(*metadata)) {
        status.to_protobuf(metadata->mutable_cdc_metadata()->mutable_capture_status());
    }
}

} // namespace starrocks::lake

#define RETURN_IF_CDC_DISABLED(metadata)               \
    do {                                               \
        if (!starrocks::lake::cdc_enabled(metadata)) { \
            return;                                    \
        }                                              \
    } while (0)

#define RETURN_VAL_IF_CDC_DISABLED(metadata, val)      \
    do {                                               \
        if (!starrocks::lake::cdc_enabled(metadata)) { \
            return val;                                \
        }                                              \
    } while (0)
