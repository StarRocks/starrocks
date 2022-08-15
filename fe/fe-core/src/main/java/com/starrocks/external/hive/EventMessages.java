// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

public class EventMessages {
    public static class Message {
        @SerializedName("db")
        private String db;
        @SerializedName("table")
        private String table;
        @SerializedName("timestamp")
        private long timestamp;

        public String getDb() {
            return db;
        }

        public String getTable() {
            return table;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    public static class AddPartitionMessage extends Message {
        @SerializedName("partitions")
        private List<Map<String, String>> partitions;

        public List<Map<String, String>> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<Map<String, String>> partitions) {
            this.partitions = partitions;
        }
    }

    public static class DropPartitionMessage extends Message {
        @SerializedName("partitions")
        private List<Map<String, String>> partitions;

        public List<Map<String, String>> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<Map<String, String>> partitions) {
            this.partitions = partitions;
        }
    }

    public static class AlterPartitionMessage extends Message {
        @SerializedName("keyValues")
        private Map<String, String> keyValues;

        public Map<String, String> getKeyValues() {
            return keyValues;
        }

        public void setKeyValues(Map<String, String> keyValues) {
            this.keyValues = keyValues;
        }
    }

    public static class InsertMessage extends Message {
        @SerializedName("partKeyVals")
        private Map<String, String> partKeyValues;
        @SerializedName("files")
        private List<String> files;

        public Map<String, String> getPartKeyValues() {
            return partKeyValues;
        }

        public List<String> getFiles() {
            return files;
        }

        public void setPartKeyValues(Map<String, String> partKeyValues) {
            this.partKeyValues = partKeyValues;
        }

        public void setFiles(List<String> files) {
            this.files = files;
        }
    }
}
