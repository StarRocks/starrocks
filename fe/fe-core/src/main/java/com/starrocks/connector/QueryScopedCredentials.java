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

package com.starrocks.connector;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Credentials that were issued for one table, for one planning attempt, and for the principal that ran it.
 *
 * The planner and the coordinator only see this interface; who issued the credentials and how they are
 * renewed stays inside the connector. Implementations are immutable - renewal produces a new instance rather
 * than mutating one a scan node may already have serialized.
 *
 * Two rules callers must not work around: every accessor validates before it answers, and a failed
 * validation throws. Returning null or an empty configuration would be read as "no credentials needed" by
 * code that predates this interface.
 */
public interface QueryScopedCredentials {

    /**
     * Same query, same execution attempt. A boolean because the analyzer asks it to decide whether a
     * resolved table can be reused; anything about to use the credentials calls {@link #cloudConfiguration}.
     */
    boolean matches(ConnectContext context);

    /**
     * The credentials to scan with, validated on every call: a plan can be serialized again after the
     * credentials behind it were replaced, and a fragment can be dispatched more often than it was built.
     *
     * @throws RuntimeException if the credentials do not belong to this context, are incomplete, or expired
     */
    CloudConfiguration cloudConfiguration(ConnectContext context);

    /**
     * How long this scan may actually run, given how long its credentials last.
     *
     * <p>The earlier of the requested deadline and the last moment these credentials can be used.
     * Deliberately not "cover the deadline or refuse": a timeout says how long a query is <i>allowed</i> to
     * run, so that would refuse a one-row INSERT as readily as a terabyte one. Refusal is for an empty
     * window only.
     *
     * @param requestedDeadline the deadline the statement's timeout implies, measured from admission
     * @throws RuntimeException if there is no usable window at all, or the credentials do not belong to this
     *         query
     */
    CredentialAdmission admitForExecution(Instant requestedDeadline);

    /**
     * Mixed into every cache key a scan through these credentials builds, so one principal's scan is not
     * served from what another's cached.
     *
     * <p>Zero means the ordinary key, byte for byte. Unlike the accessors above this validates nothing: it
     * is not a credential, and a plan that cannot be built is refused by those accessors anyway.
     */
    default long cacheScopeSeed() {
        return 0;
    }

    /**
     * The outcome of admitting one scan: when its credentials stop being usable.
     *
     * <p>A record rather than a bare {@code Instant} so that a reason, or a per-table diagnostic, can be
     * added later without changing every caller.
     *
     * @param effectiveDeadline the last moment this scan may run - never later than what was requested
     */
    record CredentialAdmission(Instant effectiveDeadline) {
        public CredentialAdmission {
            requireNonNull(effectiveDeadline, "effectiveDeadline is null");
        }
    }
}
