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
import com.google.protobuf.InvalidProtocolBufferException;
import com.staros.proto.SectionHeader;
import com.staros.proto.SectionType;
import com.staros.stream.ChunkedInputStream;
import com.staros.stream.NullInputStream;
import com.staros.util.LockCloseable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public class SectionReader implements Closeable {
    private final InputStream parent;
    private final ReentrantLock lock;
    private Section currentSection;

    public SectionReader(InputStream in) {
        this.parent = in;
        this.lock = new ReentrantLock();
        this.currentSection = null;
    }

    /**
     * Move to next available section. Possibly a SECTION_DONE, caller needs to check Section.isDone() to ensure the section is
     * valid. next() call followed by a SECTION_DONE, will yield an EOFException. Suggest use forEach() interface to keep away
     * from processing SECTION_DONE.
     *
     * @return Section object with header and stream
     * @throws IOException except from input stream, or EOFException if already reaches SECTION_DONE
     */
    public Section next() throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (currentSection != null) {
                if (currentSection.isDone()) {
                    throw new EOFException("Reaches the end of sections");
                }
                currentSection.skip();
            }
            SectionHeader header = SectionHeader.parseDelimitedFrom(parent);
            if (header == null) {
                throw new EOFException();
            }
            InputStream stream;
            if (header.getSectionType() == SectionType.SECTION_DONE) {
                stream = new NullInputStream();
            } else {
                stream = new ChunkedInputStream(parent);
            }
            currentSection = new Section(header, stream);
            return currentSection;
        } catch (InvalidProtocolBufferException exception) {
            throw new EOFException();
        }
    }

    /**
     * Convenient interface to iterate all sections, if any excepted with IOException, the iteration will be stopped.
     * <pre>
     *     SectionReader reader = new SectionReader(in);
     *     reader.forEach(x -> processSection(x));
     * </pre>
     *
     * @param action the action to accept Section as argument, possibly throws IOException
     * @throws IOException           exception in read bytes from input stream
     * @throws IllegalStateException SectionReader has been iterated with next()
     */
    public void forEach(SectionConsumer<? super Section> action) throws IOException, IllegalStateException {
        Objects.requireNonNull(action);
        if (currentSection != null) {
            throw new IllegalStateException("SectionReader has already been consumed!");
        }
        next();
        Preconditions.checkState(currentSection != null);
        while (!currentSection.isDone()) {
            action.accept(currentSection);
            next();
        }
    }

    @Override
    public void close() throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            // skip all remain sections and data, reach eof
            if (currentSection == null) {
                next();
            }
            while (!currentSection.isDone()) {
                next();
            }
        }
    }
}
