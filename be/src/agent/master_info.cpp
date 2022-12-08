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

#include "agent/master_info.h"

namespace starrocks {

static butil::DoublyBufferedData<TMasterInfo>* get_or_create_data() {
    static butil::DoublyBufferedData<TMasterInfo> obj;
    return &obj;
}

static bool update(TMasterInfo& bg, const TMasterInfo& new_value) {
    if (new_value.epoch >= bg.epoch) {
        bg = new_value;
        return true;
    } else {
        return false;
    }
}

bool get_master_info(butil::DoublyBufferedData<TMasterInfo>::ScopedPtr* ptr) {
    auto* data = get_or_create_data();
    return data->Read(ptr) == 0;
}

TMasterInfo get_master_info() {
    MasterInfoPtr ptr;
    if (get_master_info(&ptr)) {
        return *ptr;
    }
    return {};
}

bool update_master_info(const TMasterInfo& master_info) {
    auto* data = get_or_create_data();
    return data->Modify(update, master_info);
}

std::string get_master_token() {
    MasterInfoPtr ptr;
    if (get_master_info(&ptr)) {
        return ptr->token;
    }
    return "";
}

TNetworkAddress get_master_address() {
    MasterInfoPtr ptr;
    if (get_master_info(&ptr)) {
        return ptr->network_address;
    }
    return {};
}

std::optional<int64_t> get_backend_id() {
    MasterInfoPtr ptr;
    if (get_master_info(&ptr) && ptr->__isset.backend_id) {
        return ptr->backend_id;
    }
    return {};
}

} // namespace starrocks
