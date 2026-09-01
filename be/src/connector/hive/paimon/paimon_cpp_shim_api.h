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

// The contract between starrocks_be and the paimon-cpp shim library
// (libstarrocks_paimon.so). The shim is the only binary linked against
// libpaimon.so; the BE dlopen()s it lazily on the first paimon-native scan and
// resolves the two extern "C" entry points below. This header must stay free
// of paimon includes so the always-compiled loader can use it.
//
// Both entry points are only ever called by the BE build they shipped with:
// the loader compares the shim's build version with its own before touching
// anything else, so signature drift between builds is rejected up front.

namespace starrocks {

class HdfsScanner;

// Returns a heap-allocated PaimonScanner. Ownership passes to the caller,
// which releases it through HdfsScanner's virtual destructor; that is safe
// because the shim is never dlclose()d and the whole process shares the
// mem_hook malloc/free.
#define STARROCKS_PAIMON_CREATE_SCANNER_SYMBOL "starrocks_paimon_create_scanner"
using StarRocksPaimonCreateScannerFn = starrocks::HdfsScanner* (*)();

// Returns the build version baked into the shim at compile time. The loader
// rejects a shim whose build version differs from the running BE: the shim
// compiles BE class layouts (HdfsScanner, Status, Chunk, ...), so mixing
// binaries from different builds is ABI-unsafe.
#define STARROCKS_PAIMON_BUILD_VERSION_SYMBOL "starrocks_paimon_build_version"
using StarRocksPaimonBuildVersionFn = const char* (*)();

} // namespace starrocks
