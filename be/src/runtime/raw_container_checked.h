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
#include <vector>

#include "common/status.h"
#include "runtime/current_thread.h"
#include "util/raw_container.h"

namespace starrocks::raw {

template <VecContainer Container>
inline Status stl_vector_resize_uninitialized_checked(Container* vec, size_t new_size) {
    TRY_CATCH_ALLOC_SCOPE_START()
    using T = typename Container::value_type;
    ((RawVector<T, typename Container::allocator_type>*)vec)->resize(new_size);
    return Status::OK();
    TRY_CATCH_ALLOC_SCOPE_END()
}

template <VecContainer Container>
inline Status stl_vector_resize_uninitialized_checked(Container* vec, size_t reserve_size, size_t new_size) {
    TRY_CATCH_ALLOC_SCOPE_START()
    using T = typename Container::value_type;
    ((RawVector<T, typename Container::allocator_type>*)vec)->resize(reserve_size);
    vec->resize(new_size);
    return Status::OK();
    TRY_CATCH_ALLOC_SCOPE_END()
}

} // namespace starrocks::raw