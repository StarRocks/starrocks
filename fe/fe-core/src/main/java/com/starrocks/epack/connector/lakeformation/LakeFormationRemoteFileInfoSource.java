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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * A listing that owns its file systems and closes them when it runs out, when its scan is cleared, or - as a
 * backstop - when the connector metadata is discarded. Closing is idempotent.
 */
final class LakeFormationRemoteFileInfoSource implements RemoteFileInfoSource {
    private static final Logger LOG = LogManager.getLogger(LakeFormationRemoteFileInfoSource.class);

    private final RemoteFileInfoSource delegate;
    private final LakeFormationFileListing listing;
    private final AtomicBoolean closed = new AtomicBoolean();

    LakeFormationRemoteFileInfoSource(RemoteFileInfoSource delegate, LakeFormationFileListing listing) {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.listing = requireNonNull(listing, "listing is null");
    }

    /** A failing listing is over too, so it releases before the failure propagates. */
    @Override
    public RemoteFileInfo getOutput() {
        try {
            return delegate.getOutput();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /** Closes on exhaustion or failure: some paths release a coordinator without clearing its scan nodes. */
    @Override
    public boolean hasMoreOutput() {
        boolean more;
        try {
            more = delegate.hasMoreOutput();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        if (!more) {
            close();
        }
        return more;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            delegate.close();
        } catch (Exception e) {
            LOG.warn("Failed to close the delegate file listing: {}", e.toString());
        } finally {
            listing.close();
        }
    }
}
