package com.starrocks.pseudocluster;

import com.google.gson.annotations.SerializedName;

public class Rowset {
    @SerializedName(value = "id")
    int id = -1;
    @SerializedName(value = "rowsetid")
    String rowsetid;
    @SerializedName(value = "numRows")
    long numRows = 0;
    @SerializedName(value = "dataSize")
    long dataSize = 0;

    Rowset(String rowsetid) {
        this.rowsetid = rowsetid;
    }

    void setId(int id) {
        this.id = id;
    }
}
