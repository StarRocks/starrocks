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


package com.staros.journal;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.proto.JournalEntry;
import com.staros.proto.JournalHeader;
import com.staros.proto.OperationType;
import com.staros.util.AbstractParser;
import com.staros.util.Text;
import com.staros.util.Writable;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// a journal wraps an operation as log
public class Journal implements Writable {

    private final JournalEntry entry;

    public Journal(JournalEntry entry) {
        this.entry = entry;
    }

    public JournalEntry getEntry() {
        return entry;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeBytes(out, entry.toByteArray());
    }

    public long size() {
        return entry.getSerializedSize();
    }

    public static Journal read(DataInput in) throws IOException {
        return new Journal(JournalEntry.parseFrom(Text.readBytes(in)));
    }

    // Base class to help serialize to Journal and deserialize from Journal
    abstract static class AbstractJournalOp<T> extends AbstractParser<T> {

        public Journal toJournal(OperationType type, String serviceId, T data) throws StarException {
            assert type != OperationType.OP_INVALID;

            ByteArrayOutputStream out = new ByteArrayOutputStream(128);
            try {
                this.write(new DataOutputStream(out), data);
            } catch (IOException e) {
                throw new StarException(ExceptionCode.IO, e.getMessage());
            }

            JournalHeader header = JournalHeader.newBuilder()
                    .setServiceId(serviceId)
                    .setOperationType(type)
                    .build();

            JournalEntry entry = JournalEntry.newBuilder()
                    .setHeader(header)
                    .setBody(ByteString.copyFrom(out.toByteArray()))
                    .build();

            return new Journal(entry);
        }

        // Determine how to write `T data` into DataOutput
        public abstract void write(DataOutput out, T data) throws IOException;

        // parseFrom(DataInput in)
        //
        // Determines how to read bytes from `DataInput @in`, and construct `T`
        // @Note `write(DataOutput, T)` and `T parseFrom(DataInput)` must
        // implement the `T` serialization and deserialization symmetrically.
        public T parseFromJournal(OperationType type, Journal journal) throws StarException {
            try {
                assert journal.entry.getHeader().getOperationType() == type;
                return this.parseFrom(journal.entry.getBody().toByteArray());
            } catch (IOException e) {
                throw new StarException(ExceptionCode.IO, e.getMessage());
            }
        }
    }

    // Protobuf Message specialization: AbstractJournalOp<T>
    public abstract static class ProtoJournalOp<T extends Message> extends AbstractJournalOp<T> {
        // Protobuf Message turns to toByteArray() and write to @out
        @Override
        public void write(DataOutput out, T data) throws IOException {
            Text.writeBytes(out, data.toByteArray());
        }

        // @bytes raw bytes to construct the Protobuf Message
        // The protobuf message is written to DataOutput by `Message.toByteArray()`
        public abstract T read(byte[] bytes) throws InvalidProtocolBufferException;

        // bytes are read from @in and pass to read(byte[]) interface to create the Protobuf object
        @Override
        public T parseFrom(DataInput in) throws IOException {
            return this.read(Text.readBytes(in));
        }
    }

    // Writable object specialization: AbstractJournalOp<T>
    abstract static class WritableJournalOp<T extends Writable> extends AbstractJournalOp<T> {
        // data written to DataOutput using its Writable::write() interface
        @Override
        public void write(DataOutput out, T data) throws IOException {
            data.write(out);
        }
    }

    // List object specialization: AbstractJournalOp<List<T>>
    abstract static class AbstractListJournalOp<T> extends AbstractJournalOp<List<T>> {
        @Override
        public void write(DataOutput out, List<T> data) throws IOException {
            out.writeInt(data.size());
            for (T t : data) {
                writeElement(out, t);
            }
        }

        // Single Element write/parse implementation should be symmetric.
        public abstract void writeElement(DataOutput out, T data) throws IOException;

        public abstract T parseElement(DataInput in) throws IOException;

        @Override
        public List<T> parseFrom(DataInput in) throws IOException {
            int size = in.readInt();
            List<T> result = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                result.add(parseElement(in));
            }
            return result;
        }
    }

    // List of Writable objects specialization: AbstractJournalOp<List<Writable>>
    abstract static class AbstractListWritableJournalOp<T extends Writable> extends AbstractListJournalOp<T> {
        // Use Writable::write for list of Writable objects
        @Override
        public void writeElement(DataOutput out, T data) throws IOException {
            data.write(out);
        }
    }
}
