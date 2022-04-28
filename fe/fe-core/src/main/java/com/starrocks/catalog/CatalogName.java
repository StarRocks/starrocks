// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;

import java.util.Objects;

public final class CatalogName
{
    private final String catalogName;

    public static CatalogName of(String name) {
        return new CatalogName(name);
    }

    public CatalogName(String catalogName)
    {
        this.catalogName = Preconditions.checkNotNull(catalogName, "catalogName is null");
        Preconditions.checkArgument(!catalogName.isEmpty(), "catalogName is empty");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogName that = (CatalogName) o;
        return Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName);
    }

    /**
     * Returns the catalog name.
     */
    @Override
    public String toString()
    {
        return catalogName;
    }
}
