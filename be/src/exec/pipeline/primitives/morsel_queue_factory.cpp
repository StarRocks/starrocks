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

#include "exec/pipeline/primitives/morsel_queue_factory.h"

namespace starrocks::pipeline {

Status MorselQueueFactory::append_morsels(int driver_seq, Morsels&& morsels) {
    return Status::NotSupported("MorselQueueFactory::append_morsels not supported");
}

StatusOr<int> MorselQueueFactory::next_driver_seq() {
    return Status::NotSupported("MorselQueueFactory::next_driver_seq not supported");
}

Status MorselQueueFactory::mark_split_source_morsel_finished() {
    return Status::NotSupported("MorselQueueFactory::mark_split_source_morsel_finished not supported");
}

} // namespace starrocks::pipeline
