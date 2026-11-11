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

package com.starrocks.qe.scheduler;

import com.google.common.base.Preconditions;
import com.starrocks.common.FeConstants;
import com.starrocks.thrift.TPlanFragment;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * A {@link TPlanFragment} shared by the deploy requests of all the instances of one fragment.
 *
 * <p>Every instance request embeds the same plan, which can be large (e.g. every expr node carries its full
 * type, and a deeply nested struct type is written once per node). Serializing it once and copying the
 * cached bytes into each request turns N serializations of the plan into one.
 *
 * <p>The output is byte-identical to the regular serialization: in the binary and compact protocols a nested
 * struct is encoded exactly like a top-level one (the compact protocol resets the last field id to 0 when a
 * struct begins and restores it when the struct ends, and neither is visible in the bytes). Other protocols
 * fall back to the regular serialization.
 *
 * <p>The fields must not be modified after the first serialization.
 */
public class SerializeOnceTPlanFragment extends TPlanFragment {
    // One query uses one protocol (thrift_plan_protocol), so normally only one of them is filled. Each is filled once
    // under the lock and only read afterwards, so the serializations after the first one do not take the lock.
    private volatile byte[] binaryBytes;
    private volatile byte[] compactBytes;

    @Override
    public void write(TProtocol oprot) throws TException {
        // Writing cached bytes is only valid for protocols that encode a nested struct regardless of the writer state.
        // The JSON protocols need a separator from their context before a nested struct, and TTupleProtocol (a
        // subclass of TCompactProtocol) uses another scheme, so they take the regular path.
        byte[] bytes;
        if (oprot.getClass() == TBinaryProtocol.class) {
            bytes = binaryBytes;
            if (bytes == null) {
                bytes = serializeOnce(true);
            }
        } else if (oprot.getClass() == TCompactProtocol.class) {
            bytes = compactBytes;
            if (bytes == null) {
                bytes = serializeOnce(false);
            }
        } else {
            super.write(oprot);
            return;
        }
        if (FeConstants.runningUnitTest) {
            // Catch a modification after the first serialization, which the cached bytes would silently drop.
            Preconditions.checkState(Arrays.equals(bytes, serialize(oprot.getClass() == TBinaryProtocol.class)),
                    "SerializeOnceTPlanFragment is modified after it was serialized");
        }
        oprot.getTransport().write(bytes, 0, bytes.length);
    }

    // The instances of a fragment are serialized concurrently, so the lock makes the first one serialize and the
    // others wait for its bytes instead of all serializing the plan.
    private synchronized byte[] serializeOnce(boolean binary) throws TException {
        byte[] bytes = binary ? binaryBytes : compactBytes;
        if (bytes != null) {
            return bytes;
        }
        bytes = serialize(binary);
        if (binary) {
            binaryBytes = bytes;
        } else {
            compactBytes = bytes;
        }
        return bytes;
    }

    private byte[] serialize(boolean binary) throws TException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(out);
        super.write(binary ? new TBinaryProtocol(transport) : new TCompactProtocol(transport));
        return out.toByteArray();
    }
}
