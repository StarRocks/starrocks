// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TWorkGroup;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.CRC32;
// WorkGroupEntry is used by EditLog to persist WorkGroup in replicated log
// its format as follows
// |--CRC(long)--|--Length(int)--|--TWorkGroup(Byte[])--|
//
public class WorkGroupEntry implements Writable {
    TWorkGroup workgroup;

    public WorkGroupEntry(TWorkGroup workgroup) {
        this.workgroup = workgroup;
    }

    public static WorkGroupEntry read(DataInput in) throws IOException {
        long crc = in.readLong();
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        CRC32 checksum = new CRC32();
        checksum.update(bytes, 0, bytes.length);
        long actualCrc = checksum.getValue();
        if (crc != actualCrc) {
            throw new IOException("Corrupted workgroup entry");
        }
        TDeserializer deser = new TDeserializer();
        TWorkGroup wg = new TWorkGroup();
        try {
            deser.deserialize(wg, bytes);
            return new WorkGroupEntry(wg);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public TWorkGroup getWorkGroup() {
        return workgroup;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            TSerializer serializer = new TSerializer();
            byte[] bytes = serializer.serialize(workgroup);
            CRC32 checksum = new CRC32();
            checksum.update(bytes, 0, bytes.length);
            out.writeLong(checksum.getValue());
            out.writeInt(bytes.length);
            out.write(bytes, 0, bytes.length);
        } catch (TException e) {
            throw new IOException(e);
        }
    }
}
