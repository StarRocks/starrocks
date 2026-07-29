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

#include "compute_env/spill/codec/spill_codec.h"

namespace starrocks::spill {

// Installs the M1 scalar codecs (bool/int families with nullable null-bitmap framing)
// into the registry's id table. Called once from CodecRegistry's constructor.
void install_scalar_codecs(std::vector<const SpillColumnCodec*>* by_id);

} // namespace starrocks::spill
