# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0.

# Install the prebuilt dependency with thirdparty/build-thirdparty.sh lance before configuring BE.
# BE consumes the installed library and matching header without invoking Rust tools.
find_package(StarRocksLance 0.1.0 EXACT REQUIRED CONFIG
             HINTS "${StarRocksLance_ROOT}" "$ENV{StarRocksLance_ROOT}"
             PATHS "${THIRDPARTY_DIR}/lance" NO_DEFAULT_PATH)

# build.sh packages this into be/lib even when Java extensions are disabled.
install(FILES "${StarRocksLance_LIBRARY}" DESTINATION "${OUTPUT_DIR}/lib")
