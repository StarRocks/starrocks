// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

public class DatabaseName implements ParseNode {
    private String catalog;
    private String database;

    private final NodePosition pos;

    public DatabaseName(String catalog, String database) {
        this(catalog, database, NodePosition.ZERO);
    }

    public DatabaseName(String catalog, String database, NodePosition pos) {
        this.catalog = catalog;
        this.database = database;
        this.pos = pos;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return catalog + "." + database;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseName databaseName = (DatabaseName) o;
        return Objects.equals(catalog, databaseName.catalog)
                && Objects.equals(database, databaseName.database);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, database);
    }
}
