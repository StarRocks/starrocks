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


package com.starrocks.persist.metablock;

import java.io.IOException;
import java.lang.reflect.Type;

public interface SRMetaBlockReader {

    SRMetaBlockHeader getHeader();

    <T> T readJson(Class<T> returnClass) throws IOException, SRMetaBlockEOFException;

    int readInt() throws IOException, SRMetaBlockEOFException;

    long readLong() throws IOException, SRMetaBlockEOFException;

    Object readJson(Type returnType) throws IOException, SRMetaBlockEOFException;

    void close() throws IOException, SRMetaBlockException;
}