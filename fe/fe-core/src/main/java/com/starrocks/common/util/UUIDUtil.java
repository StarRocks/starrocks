// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.starrocks.thrift.TUniqueId;

import java.util.UUID;

public class UUIDUtil {
    private static final EthernetAddress ethernetAddress = EthernetAddress.fromInterface();
    private static final TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator(ethernetAddress);

    // java.util.UUID.randomUUID() uses SecureRandom to generate random uuid,
    // and SecureRandom hold locks when generate random bytes.
    // This method is faster than java.util.UUID.randomUUID() in high concurrency environment
    public static UUID genUUID() {
        return uuidGenerator.generate();
    }

    public static UUID fromTUniqueid(TUniqueId id) {
        return new UUID(id.getHi(), id.getLo());
    }

    public static TUniqueId toTUniqueId(UUID uuid) {
        return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TUniqueId genTUniqueId() {
        return toTUniqueId(uuidGenerator.generate());
    }

}
