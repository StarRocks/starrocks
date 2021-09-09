// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memory/common.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "gutil/stringprintf.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/types.h"
#include "util/time.h"

namespace starrocks {
namespace memory {

template <class T, class ST>
inline T padding(T v, ST pad) {
    return (v + pad - 1) / pad * pad;
}

template <class T, class ST>
inline size_t num_block(T v, ST bs) {
    return (v + bs - 1) / bs;
}

} // namespace memory
} // namespace starrocks
