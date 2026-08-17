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

#include <utility>
#include <vector>

#include "common/status.h"
#include "compute_env/spill/input_stream.h"

namespace starrocks::spill {

class YieldableRestoreTask {
public:
    YieldableRestoreTask(InputStreamPtr input_stream) : _input_stream(std::move(input_stream)) {
        _input_stream->get_io_stream(&_sub_stream);
    }
    Status do_read(workgroup::YieldContext& ctx, SerdeContext& context);

private:
    InputStreamPtr _input_stream;
    std::vector<SpillInputStream*> _sub_stream;
};

} // namespace starrocks::spill
