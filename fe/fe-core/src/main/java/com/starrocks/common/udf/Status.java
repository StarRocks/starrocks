package com.starrocks.common.udf;

public class Status {
    public enum ErrCode {
        OK,
        FAILED
    }

    private final ErrCode errCode;
    private final String errMsg;

    public Status(ErrCode errCode, String errMsg) {
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public static final Status
            OK = new Status(Status.ErrCode.OK, "");

    public String getErrMsg() {
        return errMsg;
    }
}