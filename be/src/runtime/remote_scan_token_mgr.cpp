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

#include "runtime/remote_scan_token_mgr.h"

#include "base/time/time.h"

namespace starrocks {

namespace {

bool is_expired(const RemoteScanTokenMgr::TokenInfo& token_info, int64_t now_ms) {
    return token_info.expire_ms > 0 && now_ms > token_info.expire_ms;
}

} // namespace

Status RemoteScanTokenMgr::register_token(std::string token, const TUniqueId& fragment_instance_id,
                                          TStarRocksScanTransport::type transport, int64_t expire_ms) {
    if (token.empty()) {
        return Status::InvalidArgument("remote scan token is empty");
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _tokens[std::move(token)] = TokenInfo{fragment_instance_id, transport, expire_ms};
    }
    return Status::OK();
}

Status RemoteScanTokenMgr::mark_completed(const std::string& token) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _tokens.find(token);
    if (iter != _tokens.end()) {
        iter->second.completed = true;
    }
    return Status::OK();
}

Status RemoteScanTokenMgr::lookup(const std::string& token, TStarRocksScanTransport::type expected_transport,
                                  TUniqueId* fragment_instance_id, bool* completed) {
    if (token.empty()) {
        return Status::InvalidArgument("remote scan token is empty");
    }

    std::lock_guard<std::mutex> l(_lock);
    auto iter = _tokens.find(token);
    if (iter == _tokens.end()) {
        return Status::NotFound("remote scan token not found");
    }
    if (is_expired(iter->second, UnixMillis())) {
        // Do NOT erase here: the background cleanup sweep reaps the token AND cancels its queue
        // together (it discovers queues only via tokens). Erasing the token on lookup would
        // orphan the queue and leak its buffered data. Just report expiry; the sweep handles it.
        return Status::NotFound("remote scan token has expired");
    }
    if (iter->second.transport != expected_transport) {
        return Status::InvalidArgument("remote scan token transport mismatch");
    }

    if (completed != nullptr) {
        *completed = iter->second.completed;
    }
    *fragment_instance_id = iter->second.fragment_instance_id;
    return Status::OK();
}

std::vector<ExpiredRemoteScanToken> RemoteScanTokenMgr::cleanup_expired_tokens(int64_t now_ms) {
    std::vector<ExpiredRemoteScanToken> expired_tokens;

    std::lock_guard<std::mutex> l(_lock);
    for (auto iter = _tokens.begin(); iter != _tokens.end();) {
        if (is_expired(iter->second, now_ms)) {
            expired_tokens.push_back(
                    ExpiredRemoteScanToken{iter->first, iter->second.fragment_instance_id, iter->second.transport});
            iter = _tokens.erase(iter);
        } else {
            ++iter;
        }
    }
    return expired_tokens;
}

Status RemoteScanTokenMgr::remove(const std::string& token) {
    std::lock_guard<std::mutex> l(_lock);
    _tokens.erase(token);
    return Status::OK();
}

size_t RemoteScanTokenMgr::size() const {
    std::lock_guard<std::mutex> l(_lock);
    return _tokens.size();
}

} // namespace starrocks
