package com.starrocks.common.udf;

import java.io.File;

public interface StorageHandler {

    void getObject(String remotePath, File localFile) throws Exception;

}