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
// glog MUST be included before gflags. Instead of including them,
// our files should include this file instead.
#include <glog/logging.h>

// Define VLOG levels.  We want display per-row info less than per-file which
// is less than per-query.  For now per-connection is the same as per-query.
#define VLOG_CONNECTION VLOG(2)
#define VLOG_RPC VLOG(8)
#define VLOG_QUERY VLOG(2)
#define VLOG_FILE VLOG(2)
#define VLOG_OPERATOR VLOG(3)
#define VLOG_ROW VLOG(10)
#define VLOG_PROGRESS VLOG(2)
#define VLOG_CACHE VLOG(3)

#define VLOG_CONNECTION_IS_ON VLOG_IS_ON(2)
#define VLOG_RPC_IS_ON VLOG_IS_ON(2)
#define VLOG_QUERY_IS_ON VLOG_IS_ON(2)
#define VLOG_FILE_IS_ON VLOG_IS_ON(2)
#define VLOG_OPERATOR_IS_ON VLOG_IS_ON(3)
#define VLOG_ROW_IS_ON VLOG_IS_ON(10)
#define VLOG_PROGRESS_IS_ON VLOG_IS_ON(2)
