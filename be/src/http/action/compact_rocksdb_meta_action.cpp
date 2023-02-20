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

#include "compact_rocksdb_meta_action.h"

#include <sstream>
#include <string>

#include "common/logging.h"
#include "http/http_request.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {

void CompactRocksDbMetaAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    _compact(req);
    LOG(INFO) << "compact rocksdb meta finished!";
}

void CompactRocksDbMetaAction::_compact(HttpRequest* req) {
    StorageEngine::instance()->do_manual_compact(true);
}

} // end namespace starrocks
