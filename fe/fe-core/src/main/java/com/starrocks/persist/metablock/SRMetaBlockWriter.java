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

public interface SRMetaBlockWriter {
    void writeJson(Object object) throws IOException, SRMetaBlockException;

    void writeInt(int value) throws IOException, SRMetaBlockException;

    void writeLong(long value) throws IOException, SRMetaBlockException;

    void writeByte(byte value) throws IOException, SRMetaBlockException;

    void writeShort(short value) throws IOException, SRMetaBlockException;

    void writeDouble(double value) throws IOException, SRMetaBlockException;

    void writeFloat(float value) throws IOException, SRMetaBlockException;

    void writeChar(char value) throws IOException, SRMetaBlockException;

    void writeBoolean(boolean value) throws IOException, SRMetaBlockException;

    void writeString(String value) throws IOException, SRMetaBlockException;

    void close() throws IOException, SRMetaBlockException;
}