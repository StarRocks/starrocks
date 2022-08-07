package com.starrocks.external;

public interface RemoteFileIOProvider {
    RemoteFileIO createRemoteFileIO();
}
