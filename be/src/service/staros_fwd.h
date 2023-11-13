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

#ifdef USE_STAROS

// Q: Why need this header file?
// A: Resolve the macro redefinition error of ANNOTATE_*. starrocks project has them defined
//   in `gutil/dynamic_annotations.h` while they are also defined in `absl/base/internal/dynamic_annotations.h`
//   as a thirdparty dependency introduced by starlet library.
namespace starrocks {

// declare a few C-style interface without knowing the detailed type definition or implementation detail.
void init_staros_worker();
void shutdown_staros_worker();
void update_staros_starcache();

// return approximate number of shards from the worker
size_t staros_worker_approx_num_shards();

} // namespace starrocks
#endif // USE_STAROS
