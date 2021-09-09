// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/wrap/zero-copy-stream-wrapper.h

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ZERO_COPY_STREAM_WRAPPER_HH
#define ZERO_COPY_STREAM_WRAPPER_HH

#include "Adaptor.hh"

DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wdeprecated")
DIAGNOSTIC_IGNORE("-Wpadded")
DIAGNOSTIC_IGNORE("-Wunused-parameter")
#endif

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#include <google/protobuf/io/zero_copy_stream.h>

DIAGNOSTIC_POP

#endif
