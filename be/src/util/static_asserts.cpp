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

#include <cstddef>

#include "runtime/datetime_value.h"

namespace starrocks {
// This class is unused.  It contains static (compile time) asserts.
// This is useful to validate struct sizes and other similar things
// at compile time.  If these asserts fail, the compile will fail.
class UnusedClass {
private:
    static_assert(sizeof(Slice) == 16, "Slice size mismatch");
    static_assert(offsetof(Slice, size) == 8, "Slice.size offset mismatch");
    // Datetime value
    static_assert(sizeof(DateTimeValue) == 16, "DateTimeValue size mismatch");
};

} // namespace starrocks
