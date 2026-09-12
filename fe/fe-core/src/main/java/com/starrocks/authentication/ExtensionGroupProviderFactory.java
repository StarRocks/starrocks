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

import com.starrocks.common.DdlException;

import java.util.Map;

/**
 * Creates the {@link GroupProvider} behind a group provider of type {@code extension}.
 * <p>
 * An extension registers its implementation under its own class while loading:
 *
 * <pre>
 * &#64;SRModule(name = "my-groups")
 * public class MyExtension implements StarRocksExtension {
 *     &#64;Override
 *     public void onLoad(ExtensionContext ctx) {
 *         ctx.register(MyGroupProviderFactory.class, new MyGroupProviderFactory());
 *     }
 * }
 *
 * public class MyGroupProviderFactory implements ExtensionGroupProviderFactory {
 *     &#64;Override
 *     public GroupProvider create(String name, Map&lt;String, String&gt; properties) throws DdlException {
 *      (...)
 *     }
 * }
 * </pre>
 *
 * and a definition then names that class, so several unrelated factories can coexist on one
 * frontend:
 *
 * <pre>{@code
 * CREATE GROUP PROVIDER my_groups PROPERTIES (
 *     "type" = "extension",
 *     "provider_factory_class" = "com.example.MyGroupProviderFactory",
 *     (...)
 * );
 * }</pre>
 *
 * @see ExtensionGroupProvider
 */
public interface ExtensionGroupProviderFactory {

    /**
     * Creates the provider for a single group provider definition.
     * <p>
     * This method is not a lifecycle hook. It runs once while the {@code CREATE GROUP PROVIDER}
     * statement is analysed and again when the definition is applied, so it may be called more than
     * once for the same statement and must stay cheap and free of side effects. Anything that
     * acquires a resource — opening a connection, starting a scheduler, reading remote state —
     * belongs in {@link GroupProvider#init()}, which is paired with
     * {@link GroupProvider#destroy()}.
     * <p>
     * The returned provider owns the validation of its own properties in
     * {@link GroupProvider#checkProperty()}; {@code provider_factory_class} is consumed before this
     * method is reached and is not the provider's concern. It may additionally implement
     * {@link AccessControlContextAwareGroupProvider} to receive the authentication context of the
     * session, which is the only way to reach information such as the token obtained during
     * authentication.
     * <p>
     * The returned provider resolves groups on the authentication path and is therefore called
     * concurrently by several sessions; its {@code getGroup} implementations must be thread-safe.
     *
     * @param name       name of the group provider being defined
     * @param properties properties of the definition, including those specific to this factory
     * @return the provider to delegate to, never {@code null}
     * @throws DdlException if the provider cannot be created in this environment; reported to the
     *                      user when a definition is created, and logged when metadata is replayed
     *                      on a frontend where the extension is missing
     */
    GroupProvider create(String name, Map<String, String> properties) throws DdlException;
}
