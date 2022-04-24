package com.starrocks.spi;

import java.util.Objects;

public class TableIdentifier {
    private String catalogName;
    private String database;
    private String table;

    public TableIdentifier(String catalogName, String database, String table) {
        this.catalogName = catalogName;
        this.database = database;
        this.table = table;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(database, that.database) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, database, table);
    }
}
