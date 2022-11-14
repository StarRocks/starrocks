// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/raw_value.h

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

#include "runtime/types.h"

namespace starrocks {

// Useful utility functions for runtime values (which are passed around as void*).
// TODO(murphy) remove it
class RawValue {
public:
    // Compares both values.
    // Return value is < 0  if v1 < v2, 0 if v1 == v2, > 0 if v1 > v2.
    static int compare(const void* v1, const void* v2, const TypeDescriptor& type);
};

} // namespace starrocks
