#!/bin/bash
set -e

# --- Configuration & Arguments ---
# $1: Version (default: 4.0.4)
# $2: FE Source directory (default: ../../out/fe)
# $3: BE/CN Source directory (default: ../../out/be)

VERSION=${1:-"4.0.4"}
ARCH="amd64"
FE_SOURCE=${2:-"../../out/fe"}
BE_SOURCE=${3:-"../../out/be"}

echo "### StarRocks Debian Packaging Build ###"

# Detect OS for portable sed
SED_I=(-i)
if [[ "$OSTYPE" == "darwin"* ]]; then
  SED_I=(-i '')
fi

# Cleanup previous builds
rm -rf target
mkdir -p target

# Processing Loop
for COMP in "fe" "be" "cn"; do
    PACKAGE_NAME="starrocks-$COMP"
    STAGING_DIR="target/${PACKAGE_NAME}_${VERSION}_${ARCH}"
    SRC_DIR=$([ "$COMP" == "fe" ] && echo "$FE_SOURCE" || echo "$BE_SOURCE")
    
    echo "--- Processing $PACKAGE_NAME ---"

    if [ ! -d "$SRC_DIR" ]; then
        echo "ERROR: Source directory $SRC_DIR not found!"
        exit 1
    fi

    # Create Structure
    mkdir -p "$STAGING_DIR/DEBIAN"
    mkdir -p "$STAGING_DIR/usr/lib/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/etc/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/var/log/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/var/lib/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/lib/systemd/system"

    # Copy Binaries/Libs
    cp -r "$SRC_DIR/bin" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    cp -r "$SRC_DIR/lib" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    
    if [ "$COMP" == "fe" ]; then
        [ -d "$SRC_DIR/spark-dpp" ] && cp -r "$SRC_DIR/spark-dpp" "$STAGING_DIR/usr/lib/starrocks/fe/"
        [ -d "$SRC_DIR/webroot" ] && cp -r "$SRC_DIR/webroot" "$STAGING_DIR/usr/lib/starrocks/fe/"
    else
        [ -d "$SRC_DIR/www" ] && cp -r "$SRC_DIR/www" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    fi

    # Copy Configs and set up symlink
    cp -r "$SRC_DIR/conf/"* "$STAGING_DIR/etc/starrocks/$COMP/"
    ln -s "/etc/starrocks/$COMP" "$STAGING_DIR/usr/lib/starrocks/$COMP/conf"

    # Cleanup either BE or CN
    if [ "$COMP" == "be" ]; then
        rm -f "$STAGING_DIR/etc/starrocks/be/cn.conf"
    elif [ "$COMP" == "cn" ]; then
        rm -f "$STAGING_DIR/etc/starrocks/cn/be.conf"
    fi

    # Path Patching (LSB)
    echo "Patching $COMP.conf for standard paths..."
    CONF_FILE="$STAGING_DIR/etc/starrocks/$COMP/$COMP.conf"

    # Define the key and path based on component
    if [ "$COMP" == "fe" ]; then
        KEY="meta_dir"
        VAL="/var/lib/starrocks/fe/meta"
    elif [ "$COMP" == "cn" ]; then
        KEY="storage_root_path"
        VAL="/var/lib/starrocks/cn/cache"
    else
        KEY="storage_root_path"
        VAL="/var/lib/starrocks/be/storage"
    fi

    # Robust Regex Patching
    if grep -Eq "^[[:space:]]*#?[[:space:]]*$KEY[[:space:]]*=" "$CONF_FILE"; then
        sed "${SED_I[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$KEY[[:space:]]*=.*|$KEY = $VAL|" "$CONF_FILE"
    else
        echo "$KEY = $VAL" >> "$CONF_FILE"
    fi

    # Patch common log and PID paths
    for VAR in "sys_log_dir" "LOG_DIR" "PID_DIR"; do
        VALUE="/var/log/starrocks/$COMP"
        [ "$VAR" == "PID_DIR" ] && VALUE="/run/starrocks"
        
        if grep -Eq "^[[:space:]]*#?[[:space:]]*$VAR[[:space:]]*=" "$CONF_FILE"; then
            sed "${SED_I[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$VAR[[:space:]]*=.*|$VAR = $VALUE|" "$CONF_FILE"
        else
            echo "$VAR = $VALUE" >> "$CONF_FILE"
        fi
    done

    # Inject Metadata
    cp "control.$COMP" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_I[@]}" "s|^Version:.*|Version: $VERSION|" "$STAGING_DIR/DEBIAN/control"
    echo "" >> "$STAGING_DIR/DEBIAN/control"
    cp "postinst" "$STAGING_DIR/DEBIAN/postinst"
    chmod 755 "$STAGING_DIR/DEBIAN/postinst"
    

    echo "Generating conffiles for $COMP..."
    find "$STAGING_DIR/etc/starrocks/$COMP" -type f | sed "s|^$STAGING_DIR||" | grep . > "$STAGING_DIR/DEBIAN/conffiles"

    # Inject Systemd
    if [ -f "systemd/starrocks-$COMP.service" ]; then
        cp "systemd/starrocks-$COMP.service" "$STAGING_DIR/lib/systemd/system/"
    fi

    # Build .deb
    echo "Building $PACKAGE_NAME.deb..."
    dpkg-deb --root-owner-group --build "$STAGING_DIR" "target/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
done

echo "Build Complete! Files in target/"