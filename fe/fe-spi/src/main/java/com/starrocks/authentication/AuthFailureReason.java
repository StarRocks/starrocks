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

package com.starrocks.authentication;

import java.util.Locale;

/**
 * Why an authentication failed, for the {@code authentication_failure_reason_total} metric and the
 * log.
 *
 * <p>This never reaches the client: HTTP and Arrow Flight answer every failure with a flat {@code
 * invalid_token} / 401 so that response content and timing leak nothing about which check failed.
 * The classification is carried on the exception itself rather than parsed back out of the message,
 * because messages get reworded and the classification would then silently rot.
 */
public enum AuthFailureReason {
    /** The credential could not be parsed at all (not a JWT, bad Basic encoding). */
    MALFORMED,
    /** A Bearer token parsed, but no security integration in the chain claims its issuer. */
    NO_MATCHING_SI,
    /** Signature verification failed: algorithm not allowed, unknown kid, or a bad signature. */
    SIGNATURE_FAIL,
    /** The token is past its {@code exp}. */
    EXPIRED,
    /** A claim did not match the configuration: issuer, audience, or principal. */
    CLAIM_MISMATCH,
    /** Wrong password, unknown user, failed LDAP bind - the ordinary "credentials are wrong" case. */
    BAD_CREDENTIAL,
    /** The requested authentication path is switched off ({@code enable_rest_bearer_authentication=false}). */
    DISABLED,
    /** Not the user's fault: JWKS fetch failed, IdP unreachable, and the like. */
    INTERNAL;

    /** Metric label value, e.g. {@code no_matching_si}. */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
