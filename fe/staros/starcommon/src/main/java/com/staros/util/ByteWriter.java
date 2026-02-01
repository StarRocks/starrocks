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


package com.staros.util;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/*
 * ByteWriter
 *  In-memory OutputStream that supports variant write interfaces and throws StarException
 *  rather than IOException
 */
public class ByteWriter extends DataOutputStream {
    private static final int BUFFER_INIT_SIZE = 256;

    public ByteWriter() {
        super(new ByteArrayOutputStream(BUFFER_INIT_SIZE));
    }

    public byte[] getData() {
        return ((ByteArrayOutputStream) out).toByteArray();
    }

    public void write(Writable w) throws StarException {
        try {
            w.write(this);
        } catch (IOException e) {
            throw new StarException(ExceptionCode.IO, e.getMessage());
        }
    }

    public void writeI(int i) throws StarException {
        try {
            writeInt(i);
        } catch (IOException e) {
            throw new StarException(ExceptionCode.IO, e.getMessage());
        }
    }

    public void writeL(long l) throws StarException {
        try {
            writeLong(l);
        } catch (IOException e) {
            throw new StarException(ExceptionCode.IO, e.getMessage());
        }
    }

    public void writeB(byte[] bytes) throws StarException {
        try {
            Text.writeBytes(this, bytes);
        } catch (IOException e) {
            throw new StarException(ExceptionCode.IO, e.getMessage());
        }
    }
}
