package io.airbyte.integrations.destination.starrocks;

public class StreamLoadProperties {
    private String database;
    private String table;
    private String[] feHost;
    private int httpPort;
    private String user;
    private String password;

    public StreamLoadProperties(String database, String table, String[] feHost, int httpPort, String user, String password) {
        this.database = database;
        this.table = table;
        this.feHost = feHost;
        this.httpPort = httpPort;
        this.user = user;
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String[] getFeHost() {
        return feHost;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
