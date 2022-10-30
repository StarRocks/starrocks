// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <functional>
#include <future>
#include <memory>

#include "common/status.h"

namespace starrocks {
class RuntimeState;
using PromiseStatus = std::promise<Status>;
using PromiseStatusPtr = std::unique_ptr<PromiseStatus>;
// if current thread was pthread. call func in current thread and return promise
// if current thread was bthread. call func in udf_thread and return promise
PromiseStatusPtr call_function_in_pthread(RuntimeState* state, std::function<Status()> func);
PromiseStatusPtr call_hdfs_scan_function_in_pthread(std::function<Status()> func);

} // namespace starrocks
