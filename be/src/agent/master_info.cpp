// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
