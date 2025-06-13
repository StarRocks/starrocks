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

    int readInt() throws IOException, SRMetaBlockEOFException;

    long readLong() throws IOException, SRMetaBlockEOFException;

    byte readByte() throws IOException, SRMetaBlockEOFException;

    short readShort() throws IOException, SRMetaBlockEOFException;

    double readDouble() throws IOException, SRMetaBlockEOFException;

    float readFloat() throws IOException, SRMetaBlockEOFException;

    char readChar() throws IOException, SRMetaBlockEOFException;

    boolean readBoolean() throws IOException, SRMetaBlockEOFException;

    String readString() throws IOException, SRMetaBlockEOFException;

    <T> T readJson(Type returnType) throws IOException, SRMetaBlockEOFException;

    <T> T readJson(Class<T> classType) throws IOException, SRMetaBlockEOFException;

    <T> void readCollection(Class<T> classType, CollectionConsumer<? super T> action) throws IOException, SRMetaBlockEOFException;

    <K, V> void readMap(Type keyType, Type valueType, MapEntryConsumer<? super K, ? super V> action)
            throws IOException, SRMetaBlockEOFException;

    void close() throws IOException, SRMetaBlockException;
}