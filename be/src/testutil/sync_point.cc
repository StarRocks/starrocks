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
//
// The code in this file was modified from the RocksDB project at the following URL:
// https://github.com/facebook/rocksdb/blob/fb63d9b4ee26afc81810bd5398bda4f2b351b26b/test_util/sync_point.cc
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "testutil/sync_point.h"

#include <fcntl.h>

#include "testutil/sync_point_impl.h"

std::vector<std::string> starrocks_kill_exclude_prefixes;

#if !defined(NDEBUG) || defined(BE_TEST)
namespace starrocks {

SyncPoint* SyncPoint::GetInstance() {
    static SyncPoint sync_point;
    return &sync_point;
}

SyncPoint::SyncPoint() : impl_(new Data) {}

SyncPoint::~SyncPoint() {
    delete impl_;
}

void SyncPoint::LoadDependency(const std::vector<SyncPointPair>& dependencies) {
    impl_->LoadDependency(dependencies);
}

void SyncPoint::LoadDependencyAndMarkers(const std::vector<SyncPointPair>& dependencies,
                                         const std::vector<SyncPointPair>& markers) {
    impl_->LoadDependencyAndMarkers(dependencies, markers);
}

void SyncPoint::SetCallBack(const std::string& point, const std::function<void(void*)>& callback) {
    impl_->SetCallBack(point, callback);
}

void SyncPoint::ClearCallBack(const std::string& point) {
    impl_->ClearCallBack(point);
}

void SyncPoint::ClearAllCallBacks() {
    impl_->ClearAllCallBacks();
}

void SyncPoint::EnableProcessing() {
    impl_->EnableProcessing();
}

void SyncPoint::DisableProcessing() {
    impl_->DisableProcessing();
}

void SyncPoint::ClearTrace() {
    impl_->ClearTrace();
}

void SyncPoint::Process(const std::string_view& point, void* cb_arg) {
    impl_->Process(point, cb_arg);
}

} // namespace starrocks
#endif // NDEBUG
