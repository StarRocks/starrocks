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

// Shim for <mach/arm/vm_param.h> on macOS ARM64.
//
// The real header defines PAGE_SIZE as a macro (vm_page_size), which clobbers
// any C++ constant or enum named PAGE_SIZE (e.g. AlignedBuffer::PAGE_SIZE in
// exec/spill/serde.h and the local #define in system_allocator.cpp).
// We include the real header then immediately undef the problematic macro.

#pragma once

#include_next <mach/arm/vm_param.h>

#undef PAGE_SIZE
