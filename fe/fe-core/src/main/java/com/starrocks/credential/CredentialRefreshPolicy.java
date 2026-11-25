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

package com.starrocks.credential;

/**
 * When a running scan should ask for renewed credentials: early enough that the renewal is in place well
 * before the old ones expire, and never so early on a short-lived credential that it renews constantly.
 */
public final class CredentialRefreshPolicy {
    // Refresh this long before expiry, or halfway through a shorter remaining lifetime.
    public static final long MAX_REFRESH_MARGIN_MS = 15 * 60_000L;
    // After a failed renewal, how long the coordinator answers with that failure instead of asking the catalog
    // again for every backend that is waiting.
    public static final long FAILED_RENEWAL_RETRY_MS = 30_000L;

    private CredentialRefreshPolicy() {
    }

    /**
     * Computed once, when the credentials arrive: measured from any later moment, half the remaining lifetime
     * would keep moving out and never come due.
     *
     * @return epoch millis after which to refresh credentials received at {@code receivedAtMs} that expire at
     *         {@code expiresAtMs}; {@code receivedAtMs} when they had already expired
     */
    public static long refreshAfterMs(long expiresAtMs, long receivedAtMs) {
        long lifetimeMs = expiresAtMs - receivedAtMs;
        if (lifetimeMs <= 0) {
            return receivedAtMs;
        }
        return expiresAtMs - Math.min(MAX_REFRESH_MARGIN_MS, lifetimeMs / 2);
    }
}
