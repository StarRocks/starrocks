package com.starrocks.external;

public class HiveRemoteFileIOProvider implements RemoteFileIOProvider {
    private HiveRemoteFileIO hiveRemoteFileIO;

    public HiveRemoteFileIOProvider(HiveRemoteFileIO hiveRemoteFileIO) {
        this.hiveRemoteFileIO = hiveRemoteFileIO;
    }

    @Override
    public RemoteFileIO createRemoteFileIO() {
        return hiveRemoteFileIO;
    }
}
