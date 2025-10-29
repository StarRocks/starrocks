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

package com.staros.section;

import com.google.common.base.Preconditions;
import com.staros.proto.SectionHeader;
import com.staros.proto.SectionType;
import com.staros.stream.ChunkedOutputStream;
import com.staros.util.LockCloseable;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantLock;

public class SectionWriter implements Closeable {
    private final OutputStream parent;
    private final ReentrantLock lock;
    private boolean closed;
    private OutputStream currentSubStream;

    public SectionWriter(OutputStream out) {
        this.parent = out;
        this.lock = new ReentrantLock();
        this.closed = false;
        this.currentSubStream = null;
    }

    public OutputStream appendSection(SectionType type) throws IOException {
        Preconditions.checkState(type != SectionType.SECTION_DONE);
        try (LockCloseable ignored = new LockCloseable(lock)) {
            disposeCurrentStream();
            writeSectionHeader(parent, type);
            // provide a BufferedOutputStream to avoid byte by byte written from caller
            currentSubStream = new BufferedOutputStream(new ChunkedOutputStream(parent));
            return currentSubStream;
        }
    }

    @Override
    public void close() throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (closed) {
                return;
            }
            disposeCurrentStream();
            writeSectionHeader(parent, SectionType.SECTION_DONE);
            closed = true;
        }
    }

    private void disposeCurrentStream() throws IOException {
        if (currentSubStream != null) {
            currentSubStream.close();
            currentSubStream = null;
        }
    }

    private static void writeSectionHeader(OutputStream os, SectionType type) throws IOException {
        SectionHeader.newBuilder().setSectionType(type).build().writeDelimitedTo(os);
    }
}
