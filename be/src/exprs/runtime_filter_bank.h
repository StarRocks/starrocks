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

// Compatibility shim: keep the legacy include path while runtime filter
// implementation lives in src/runtime/runtime_filter/*.
#include "base/concurrency/blocking_queue.hpp"
#include "base/failpoint/fail_point.h"
#include "base/uid_util.h"
#include "column/column.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exec/pipeline/schedule/observer.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter/runtime_filter_descriptor.h"
#include "runtime/runtime_filter/runtime_filter_helper.h"
#include "runtime/runtime_filter/runtime_filter_probe.h"
#include "runtime/runtime_filter_layout.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
