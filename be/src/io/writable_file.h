// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "io/output_stream.h"

namespace starrocks::io {

class WritableaFile : public OutputStream {
public:
    ~WritableFile() override = default;

    // Synchronize a file's in-core state with storage device (or other permanent storage device)
    // so that all changed information can be retrieved even after the system crashed or was rebooted.
    // It also flushes metadata information associated with the file.
    // Calling sync() does not necessarily ensure that the entry in the directory containing the file
    // has also reached disk. For that an explicit sync on the directory is also needed
    virtual Status sync() = 0;

    // Similar to `sync()`, but does not flush modified metadata unless that metadata is needed in order
    // to allow a subsequent data retrieval to be correctly handled.
    virtual Status sync_data() = 0;

    // Flushes the output stream(writing any buffered data using `flush()`) and releasing the underlying
    // resource.
    // Any further access (including another call to close()) to the stream results in undefined behavior.
    virtual Status close() = 0;
};

} // namespace starrocks::io
