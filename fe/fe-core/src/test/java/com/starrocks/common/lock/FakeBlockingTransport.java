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
package com.starrocks.common.lock;

import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;

/**
 * Stands in for a transport such as {@code HiveMetaClient} or {@code ThriftRPCRequestExecutor}: the
 * guard is called from inside it, and its own frames must not be what the report points at.
 * <p>
 * A top-level class on purpose. A nested helper would share a top-level name with the test that
 * calls it, and the guard treats nested classes as part of the same layer -- which is what makes
 * {@code HiveMetaClient$RecyclableClient} skip correctly, and would make this test vacuous.
 */
public class FakeBlockingTransport {
    public static final String TAG = "fake-transport";

    private FakeBlockingTransport() {
    }

    /** The guarded entry point. */
    public static void contact() {
        BlockingCallValidator.validateNotUnderLock(TAG);
    }

    /** An extra frame of the transport's own, as a real one has between its overloads. */
    public static void contactThroughOwnOverload() {
        contact();
    }

    /** And one more, from a lambda inside the transport. */
    public static void contactThroughOwnLambda() {
        Runnable r = FakeBlockingTransport::contact;
        r.run();
    }

    /** And from a nested class of the transport, as the HMS client's recyclable client does. */
    public static void contactThroughNestedClass() {
        Inner.contact();
    }

    private static final class Inner {
        private static void contact() {
            BlockingCallValidator.validateNotUnderLock(TAG);
        }
    }
}
