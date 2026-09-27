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

package com.starrocks.type;

/**
 * FILE is an indivisible scalar reference to a file (Parquet FILE logical type shape:
 * uri, offset, size, content_type, checksum, inline). Its six fields are fixed and are
 * materialized only inside the BE; the FE treats it as an opaque scalar.
 */
public class FileType extends ScalarType {
    public static final FileType FILE = new FileType();

    public FileType() {
        super(PrimitiveType.FILE);
    }
}
