// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/dummy_runtime_profile.h

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

#ifndef STARROCKS_BE_SRC_UTIL_DEBUG_RUNTIME_PROFILE_H
#define STARROCKS_BE_SRC_UTIL_DEBUG_RUNTIME_PROFILE_H

#include "common/object_pool.h"
#include "util/runtime_profile.h"

namespace starrocks {
class DummyProfile {
public:
    DummyProfile() : _profile(new RuntimeProfile("dummy", false)) {}
    RuntimeProfile* profile() { return _profile; }
    virtual ~DummyProfile() { delete _profile; }

private:
    ObjectPool _pool;
    RuntimeProfile* const _profile;
};
} // namespace starrocks
#endif
