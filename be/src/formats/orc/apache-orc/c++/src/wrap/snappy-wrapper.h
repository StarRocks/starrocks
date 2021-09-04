// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/wrap/snappy-wrapper.h

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

#ifndef SNAPPY_WRAPPER_HH
#define SNAPPY_WRAPPER_HH

#include "Adaptor.hh"

DIAGNOSTIC_PUSH

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#include <snappy.h>

DIAGNOSTIC_POP

#endif
