#!/bin/bash
set -e

# --- Configuration ---
VERSION="4.0.4"
ARCH="amd64"
TARBALL="../../build_output/StarRocks-4.0.4-ubuntu-amd64.tar.gz"
EXTRACT_DIR="target/extract"

echo "### StarRocks Debian Packaging Build ###"

# 1. Validation
if [ ! -f "$TARBALL" ]; then
    echo "ERROR: Tarball not found at $TARBALL"
    exit 1
fi

# 2. Cleanup
rm -rf target
mkdir -p "$EXTRACT_DIR"

# 3. Unpack
echo "Unpacking tarball..."
tar -xzf "$TARBALL" -C "$EXTRACT_DIR" --strip-components=1

# 4. Processing Loop
for COMP in "fe" "be"; do
    PACKAGE_NAME="starrocks-$COMP"
    STAGING_DIR="target/${PACKAGE_NAME}_${VERSION}_${ARCH}"
    
    echo "--- Processing $PACKAGE_NAME ---"

    # Create Structure
    mkdir -p "$STAGING_DIR/DEBIAN"
    mkdir -p "$STAGING_DIR/usr/lib/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/etc/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/var/log/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/var/lib/starrocks/$COMP"
    mkdir -p "$STAGING_DIR/lib/systemd/system"

    # Copy Binaries/Libs
    cp -r "$EXTRACT_DIR/$COMP/bin" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    cp -r "$EXTRACT_DIR/$COMP/lib" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    
    if [ "$COMP" == "fe" ]; then
        [ -d "$EXTRACT_DIR/fe/spark-dpp" ] && cp -r "$EXTRACT_DIR/fe/spark-dpp" "$STAGING_DIR/usr/lib/starrocks/fe/"
        [ -d "$EXTRACT_DIR/fe/webroot" ] && cp -r "$EXTRACT_DIR/fe/webroot" "$STAGING_DIR/usr/lib/starrocks/fe/"
    else
        [ -d "$EXTRACT_DIR/be/www" ] && cp -r "$EXTRACT_DIR/be/www" "$STAGING_DIR/usr/lib/starrocks/be/"
    fi

    # Copy Configs
    cp -r "$EXTRACT_DIR/$COMP/conf/"* "$STAGING_DIR/etc/starrocks/$COMP/"

    # Path Patching (LSB)
    echo "Patching $COMP.conf for standard paths..."
    if [ "$COMP" == "fe" ]; then
        sed -i '' "s|^#\?meta_dir.*|meta_dir = /var/lib/starrocks/fe/meta|" "$STAGING_DIR/etc/starrocks/fe/fe.conf"
        sed -i '' "s|^#\?sys_log_dir.*|sys_log_dir = /var/log/starrocks/fe|" "$STAGING_DIR/etc/starrocks/fe/fe.conf"
    else
        sed -i '' "s|^#\?storage_root_path.*|storage_root_path = /var/lib/starrocks/be/storage|" "$STAGING_DIR/etc/starrocks/be/be.conf"
        sed -i '' "s|^#\?sys_log_dir.*|sys_log_dir = /var/log/starrocks/be|" "$STAGING_DIR/etc/starrocks/be/be.conf"
    fi

    # Inject Metadata
    cp "control.$COMP" "$STAGING_DIR/DEBIAN/control"
    echo "" >> "$STAGING_DIR/DEBIAN/control"
    cp "postinst" "$STAGING_DIR/DEBIAN/postinst"
    chmod 755 "$STAGING_DIR/DEBIAN/postinst"

    if [ -f "conffiles.$COMP" ]; then
        cp "conffiles.$COMP" "$STAGING_DIR/DEBIAN/conffiles"
        echo "" >> "$STAGING_DIR/DEBIAN/conffiles"
    fi

    # Inject Systemd
    if [ -f "systemd/starrocks-$COMP.service" ]; then
        cp "systemd/starrocks-$COMP.service" "$STAGING_DIR/lib/systemd/system/"
    fi

    # Build .deb
    echo "Building $PACKAGE_NAME.deb..."
    dpkg-deb --root-owner-group --build "$STAGING_DIR" "target/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
done

echo "Build Complete! Files in target/"