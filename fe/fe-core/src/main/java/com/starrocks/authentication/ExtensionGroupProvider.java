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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class ExtensionGroupProvider extends GroupProvider
        implements AccessControlContextAwareGroupProvider {
    public static final String TYPE = "extension";
    public static final String PROVIDER_FACTORY_CLASS_PROPERTY = "provider_factory_class";
    private static final Logger LOG = LogManager.getLogger(ExtensionGroupProvider.class);
    private transient volatile GroupProvider delegate;
    private transient volatile boolean unresolvable;

    public ExtensionGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
    }

    private static GroupProvider createDelegate(String name, Map<String, String> properties)
            throws DdlException {
        String factoryClassName = properties.get(PROVIDER_FACTORY_CLASS_PROPERTY);
        if (factoryClassName == null || factoryClassName.isBlank()) {
            throw new DdlException(
                    "Missing required extension group provider property: " + PROVIDER_FACTORY_CLASS_PROPERTY);
        }

        Class<?> factoryClass;
        try {
            factoryClass = Class.forName(factoryClassName);
        } catch (ClassNotFoundException e) {
            throw new DdlException(
                    "Extension group provider factory class not found: " + factoryClassName, e);
        }
        if (!ExtensionGroupProviderFactory.class.isAssignableFrom(factoryClass)) {
            throw new DdlException(
                    "Extension component does not implement ExtensionGroupProviderFactory: "
                            + factoryClassName);
        }

        GroupProvider provider;
        try {
            Object component = ExtensionManager.getComponent(factoryClass);
            provider = ((ExtensionGroupProviderFactory) component).create(name, properties);
        } catch (RuntimeException e) {
            throw new DdlException(
                    "Extension group provider factory failed: " + factoryClassName, e);
        }

        if (provider == null) {
            throw new DdlException(
                    "Extension group provider factory returned null: " + factoryClassName);
        }
        return provider;
    }

    /**
     * Runs when the definition is applied and when metadata is replayed. DdlException is the
     * contract of the base class and is what replayCreateGroupProvider already handles, so a
     * frontend without the extension skips the provider instead of aborting journal replay.
     */
    @Override
    public void init() throws DdlException {
        this.delegate().init();
    }

    @Override
    public void destroy() {
        // A provider that was never used must not be created (using delegate()) just to be torn down.
        GroupProvider resolved = delegate;
        if (resolved != null) {
            resolved.destroy();
        }
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
        return getGroup(userIdentity, distinguishedName, null);
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName,
                                AccessControlContext accessControlContext) {
        GroupProvider resolved;
        try {
            resolved = delegate();
        } catch (DdlException e) {
            // This contract declares no checked exception, and one undeployed provider must not
            // fail an entire login process.
            LOG.debug("group provider [{}] resolves to no groups", getName(), e);
            return Set.of();
        }

        if (accessControlContext != null && resolved instanceof AccessControlContextAwareGroupProvider awareGroupProvider) {
            return awareGroupProvider.getGroup(userIdentity, distinguishedName, accessControlContext);
        }
        // delegate does not consume the session context
        return resolved.getGroup(userIdentity, distinguishedName);
    }

    @Override
    public void checkProperty() throws SemanticException {
        try {
            delegate().checkProperty();
        } catch (DdlException e) {
            throw new SemanticException(e.getMessage(), e);
        }
    }

    private GroupProvider delegate() throws DdlException {
        GroupProvider resolved = delegate;
        if (resolved != null) {
            return resolved;
        }
        if (unresolvable) {
            throw new DdlException("group provider [" + getName() + "] is not available on this frontend");
        }

        synchronized (this) {
            if (delegate == null) {
                try {
                    delegate = createDelegate(getName(), getProperties());
                } catch (DdlException e) {
                    unresolvable = true;
                    LOG.warn("group provider [{}] is not available on this frontend: {}",
                            getName(), e.getMessage());
                    LOG.debug("group provider [{}] failed to resolve its delegate", getName(), e);
                    throw e;
                }
            }
            return delegate;
        }
    }
}
