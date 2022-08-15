// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "agent/client_cache.h"

#include "gen_cpp/FrontendService.h"

namespace starrocks {

FrontendServiceClientCache g_frontend_service_client_cache;

} // namespace starrocks