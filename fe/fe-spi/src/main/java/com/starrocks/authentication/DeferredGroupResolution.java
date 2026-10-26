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
 * The part of a login that decides the session's groups: merging the group sources, deriving the
 * roles, and the {@code permitted_groups} gate. Handed to a provider that cannot run it at
 * authentication time; see {@link AuthenticationProvider#isGroupResolutionDeferred()}.
 */
@FunctionalInterface
public interface DeferredGroupResolution {
    /**
     * @throws AuthenticationException when the login must be refused, typically because none of
     *                                 its groups is on the {@code permitted_groups} list
     */
    void resolve() throws AuthenticationException;
}
