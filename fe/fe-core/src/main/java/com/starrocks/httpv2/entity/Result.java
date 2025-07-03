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


package com.starrocks.httpv2.entity;

import com.starrocks.httpv2.enums.Status;

/**
 * result
 *
 * @param <T> T
 */
public class Result<T> {
    /**
     * status
     */
    private Integer code;

    /**
     * message
     */
    private String msg;

    /**
     * data
     */
    private T data;

    public Result() {
    }

    public Result(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }


    public Result(Status status) {
        if (status != null) {
            this.code = status.getCode();
            this.msg = status.getMsg();
        }
    }


    public Result(Integer code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }


    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }


    public static <T> Result<T> success(T data) {
        return new Result<>(Status.SUCCESS.getCode(), Status.SUCCESS.getMsg(), data);
    }

    public static <T> Result<T> success() {
        return success(null);
    }


    public static <T> Result<T> error(Status status) {
        return new Result<>(status);
    }

    public static <T> Result<T> error(String msg) {
        return new Result<>(null, msg, null);
    }


    @Override
    public String toString() {
        return "Status{"
                + "code='" + code
                + '\'' + ", msg='"
                + msg + '\''
                + ", data=" + data
                + '}';
    }

}
