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

package com.starrocks.persist.image;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Reads one section of a format-v3 image. Handed to a {@link SectionLoader} by
 * {@link ImageReaderV3#load}.
 */
public interface SectionReader {

    SectionId id();

    /** Total number of entries recorded in the index for this section. */
    long numEntries();

    /**
     * Reads the manager message. The section must have one (the writer always puts it first) and
     * its frame must hold nothing else, otherwise an {@link ImageFormatException} is thrown.
     */
    <M extends Message> M readManager(Parser<M> parser) throws IOException;

    /**
     * Reads every entry of the section, in file order. The method returns after the last entry has
     * been consumed, so post-processing that depends on the whole section can follow the call
     * directly. The entries frame must hold exactly the number of entries the index declares;
     * fewer or more is an {@link ImageFormatException}.
     */
    <E extends Message> void readEntries(Parser<E> parser, Consumer<E> consume) throws IOException;
}
