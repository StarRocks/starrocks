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

/**
 * The access channel a credential arrived on.
 *
 * <p>This is orthogonal to {@link AuthCredentialKind}: the kind says what the credential
 * <em>looks like</em>, the channel says <em>which way it came in</em>. A single kind spans
 * several channels - {@code PLAIN_PASSWORD} arrives over HTTP Basic, Arrow Flight Basic and the
 * BE-&gt;FE load RPCs alike - and operations needs to tell them apart ("is HTTP failing, or is it
 * Arrow Flight?" is the first question when authentication starts erroring).
 *
 * <p>Every entry point that builds an {@link AuthCredentials} must state its channel explicitly;
 * the factory methods take no default, so the compiler forces a new channel to declare itself
 * rather than silently inheriting a wrong label.
 */
public enum AuthChannel {
    /** MySQL protocol (query port, default 9030). */
    MYSQL,
    /** FE HTTP: REST API, Web UI, and Stream Load that goes through the FE. */
    HTTP,
    /** Arrow Flight SQL (default 9408). */
    ARROW_FLIGHT,
    /** BE-&gt;FE load RPCs: stream / transaction load and merge commit. */
    THRIFT_LOAD,
    /** BE-&gt;FE generic {@code checkAuth} gate: management and observability endpoints. */
    THRIFT_GATEWAY
}
