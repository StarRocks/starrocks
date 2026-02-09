#!/bin/bash
set -e

# --- Configuration & Arguments ---
# $1: Version (default: 4.0.4)
# $2: FE Source directory (default: ../../out/fe)
# $3: BE Source directory (default: ../../out/be)

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
for COMP in "fe" "be"; do
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
        [ -d "$SRC_DIR/www" ] && cp -r "$SRC_DIR/www" "$STAGING_DIR/usr/lib/starrocks/be/"
    fi

    # Copy Configs and set up symlink
    cp -r "$SRC_DIR/conf/"* "$STAGING_DIR/etc/starrocks/$COMP/"
    ln -s "/etc/starrocks/$COMP" "$STAGING_DIR/usr/lib/starrocks/$COMP/conf"

    # Path Patching (LSB)
    echo "Patching $COMP.conf for standard paths..."
    CONF_FILE="$STAGING_DIR/etc/starrocks/$COMP/$COMP.conf"

    if [ "$COMP" == "fe" ]; then
        sed "${SED_I[@]}" "s|^#\?meta_dir.*|meta_dir = /var/lib/starrocks/fe/meta|" "$CONF_FILE"
    else
        sed "${SED_I[@]}" "s|^#\?storage_root_path.*|storage_root_path = /var/lib/starrocks/be/storage|" "$CONF_FILE"
    fi

    # Patch common log and PID paths
    sed "${SED_I[@]}" "s|^#\?sys_log_dir.*|sys_log_dir = /var/log/starrocks/$COMP|" "$CONF_FILE"
    sed "${SED_I[@]}" "s|^#\?LOG_DIR.*|LOG_DIR = /var/log/starrocks/$COMP|" "$CONF_FILE"
    sed "${SED_I[@]}" "s|^#\?PID_DIR.*|PID_DIR = /run/starrocks|" "$CONF_FILE"

    # Inject Metadata
    cp "control.$COMP" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_I[@]}" "s|^Version:.*|Version: $VERSION|" "$STAGING_DIR/DEBIAN/control"
    echo "" >> "$STAGING_DIR/DEBIAN/control"
    cp "postinst" "$STAGING_DIR/DEBIAN/postinst"
    chmod 755 "$STAGING_DIR/DEBIAN/postinst"
    

    echo "Generating conffiles for $COMP..."
    find "$STAGING_DIR/etc/starrocks/$COMP" -type f | sed "s|^$STAGING_DIR||" > "$STAGING_DIR/DEBIAN/conffiles"
    echo "" >> "$STAGING_DIR/DEBIAN/conffiles"

    # Inject Systemd
    if [ -f "systemd/starrocks-$COMP.service" ]; then
        cp "systemd/starrocks-$COMP.service" "$STAGING_DIR/lib/systemd/system/"
    fi

    # Build .deb
    echo "Building $PACKAGE_NAME.deb..."
    dpkg-deb --root-owner-group --build "$STAGING_DIR" "target/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
done

echo "Build Complete! Files in target/"