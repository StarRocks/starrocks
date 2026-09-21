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

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes one section of a format-v3 image. Obtained from {@link ImageWriterV3#beginSection}.
 *
 * <p>A section is an optional manager message followed by any number of entries. Compression and
 * the index record are handled here; callers only hand over messages. The manager message, when
 * present, must be written before the first entry and at most once.
 */
public interface SectionWriter extends Closeable {

    /** Writes the manager message. Allowed once, and only before any entry. */
    void writeManager(Message manager) throws IOException;

    /** Appends one entry. */
    void writeEntry(Message entry) throws IOException;

    /** Seals the section and records its index entry. Idempotent. */
    @Override
    void close() throws IOException;
}
