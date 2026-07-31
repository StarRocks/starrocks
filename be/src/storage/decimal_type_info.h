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

#include "storage/types.h"

namespace starrocks {

<<<<<<< HEAD:be/src/storage/decimal_type_info.h
TypeInfoPtr get_decimal_type_info(LogicalType type, int precision, int scale);

std::string get_decimal_zone_map_string(TypeInfo* type_info, const void* value);

} // namespace starrocks
=======
// Whether ThreadPool swallows an exception thrown by a task and keeps the worker running.
//
// false (default): the task body has no enclosing catch clause, so an escaping exception
// finds no handler and terminates the process at the throw point. Loud, and no task can
// report success without having produced a result.
//
// true: the exception is logged and the worker moves on to the next task. This keeps the
// process alive but does NOT make the task exception safe -- a task whose result write is
// skipped while its completion signal still fires is reported to its waiter as success.
// Only turn this on to mitigate a crash loop, and expect the failure to become silent.
CONF_mBool(enable_threadpool_catch_task_exception, "false");

} // namespace starrocks::config
>>>>>>> 546210a3be ([BugFix] Do not swallow task exceptions in ThreadPool by default (#76863)):be/src/common/config_thread_fwd.h
