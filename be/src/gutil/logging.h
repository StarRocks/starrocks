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

// GLOG defines this based on the system but doesn't check if it's already
// been defined. Undef it first to avoid warnings.
#undef _XOPEN_SOURCE
// This is including a glog internal file. We want this to expose the
// function to get the stack trace.
#include <glog/logging.h>
#undef MutexLock
