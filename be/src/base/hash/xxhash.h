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

// Inline all xxhash function definitions so the BE binary does not pull
// XXH* symbols from other archives (librdkafka and libvelocypack each
// bundle their own xxhash, which would otherwise cause duplicate-symbol
// link errors).
#define XXH_INLINE_ALL
#include <xxhash/xxhash.h>
