// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "compact_rocksdb_meta_action.h"

#include <sstream>
#include <string>

#include "common/logging.h"
#include "http/http_request.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"

namespace starrocks {

CompactRocksDbMetaAction::CompactRocksDbMetaAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void CompactRocksDbMetaAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    _compact(req);
    LOG(INFO) << "compact rocksdb meta finished!";
}

void CompactRocksDbMetaAction::_compact(HttpRequest *req) {
    _exec_env->storage_engine()->do_manual_compact(true);
}

} // end namespace starrocks