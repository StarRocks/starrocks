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

import com.staros.proto.SectionHeader;
import com.staros.proto.SectionType;

import java.io.IOException;
import java.io.InputStream;

public class Section {
    private final SectionHeader header;
    private final InputStream stream;

    public Section(SectionHeader header, InputStream stream) {
        this.header = header;
        this.stream = stream;
    }

    public void skip() throws IOException {
        if (stream == null) {
            return;
        }
        // TODO: more efficient way to drain the stream
        while (stream.read() != -1) {
            // drain the stream
        }
    }

    public SectionHeader getHeader() {
        return header;
    }

    public InputStream getStream() {
        return stream;
    }

    public boolean isDone() {
        return header.getSectionType() == SectionType.SECTION_DONE;
    }
}
