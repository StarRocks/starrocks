#!/usr/bin/env bash
set -e

# --- StarRocks Debian Packaging Build Script ---
# Purpose: Generates .deb packages for FE, BE, and CN components.
# Usage: ./build.sh [version] [fe_src] [be_src] [arch] [cn_src]
# Example: ./build.sh 4.0.4 ../../out/fe ../../out/be amd64 ../../out/be
#
# Parameters:
# $1: Version (default: 4.0.4)
# $2: FE Source directory (default: ../../out/fe)
# $3: BE Source directory (default: ../../out/be)
# $4: Architecture (default: output of dpkg --print-architecture)
# $5: CN Source directory (default: defaults to BE_SOURCE)

VERSION=${1:-"4.0.4"}
FE_SOURCE=${2:-"../../out/fe"}
BE_SOURCE=${3:-"../../out/be"}

# Determine target architecture  
if [ -n "$4" ]; then  
    ARCH="$4"  
else  
    if command -v dpkg >/dev/null 2>&1; then  
        ARCH="$(dpkg --print-architecture)"  
    else  
        # Fallback: derive a reasonable Debian architecture from uname -m  
        UNAME_M="$(uname -m)"  
        case "$UNAME_M" in  
            x86_64)  
                ARCH="amd64"  
                ;;  
            aarch64|arm64)  
                ARCH="arm64"  
                ;;  
            *)  
                echo "ERROR: Unable to determine architecture automatically (uname -m: $UNAME_M)." >&2  
                echo "Please rerun as: $0 <version> <fe_src> <be_src> <arch> [cn_src]" >&2  
                exit 1  
                ;;  
        esac  
    fi  
fi  

CN_SOURCE=${5:-"$BE_SOURCE"}
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
    
    # Determine Source Directory dynamically
    if [ "$COMP" == "fe" ]; then
        SRC_DIR="$FE_SOURCE"
    elif [ "$COMP" == "cn" ]; then
        SRC_DIR="$CN_SOURCE"
    else
        SRC_DIR="$BE_SOURCE"
    fi
    
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
    cp -a "$SRC_DIR/bin" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    cp -a "$SRC_DIR/lib" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    
    if [ "$COMP" == "fe" ]; then
        [ -d "$SRC_DIR/spark-dpp" ] && cp -a "$SRC_DIR/spark-dpp" "$STAGING_DIR/usr/lib/starrocks/fe/"
        [ -d "$SRC_DIR/webroot" ] && cp -a "$SRC_DIR/webroot" "$STAGING_DIR/usr/lib/starrocks/fe/"
    else
        [ -d "$SRC_DIR/www" ] && cp -a "$SRC_DIR/www" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    fi

    # Copy Configs and set up symlink
    cp -a "$SRC_DIR/conf/"* "$STAGING_DIR/etc/starrocks/$COMP/"
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
    if grep -Eq "^[[:space:]]*#?[[:space:]]*$KEY[[:space:]]*=[[:space:]]*\$\{STARROCKS_HOME\}.*" "$CONF_FILE"; then
        sed "${SED_I[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$KEY[[:space:]]*=[[:space:]]*\$\{STARROCKS_HOME\}.*|$KEY = $VAL|" "$CONF_FILE"
    else
        if ! grep -Eq "^[[:space:]]*$KEY[[:space:]]*=" "$CONF_FILE"; then
            echo "$KEY = $VAL" >> "$CONF_FILE"
        fi
    fi

    # Patch common log and PID paths
    for VAR in "sys_log_dir" "LOG_DIR" "PID_DIR"; do
        if [ "$VAR" == "PID_DIR" ]; then
            VALUE="/run/starrocks"
        else
            VALUE="/var/log/starrocks/$COMP"
        fi

        # Robust Regex:
        # 1. Matches lines with leading/trailing whitespace
        # 2. Handles commented or uncommented lines (#?)
        # 3. Uses -E for Extended Regex to handle [[:space:]] properly
        if grep -Eq "^[[:space:]]*#?[[:space:]]*$VAR[[:space:]]*=" "$CONF_FILE"; then
            echo "Updating $VAR in $CONF_FILE"
            sed "${SED_I[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$VAR[[:space:]]*=.*|$VAR = $VALUE|" "$CONF_FILE"
        else
            echo "Appending $VAR to $CONF_FILE"
            echo "$VAR = $VALUE" >> "$CONF_FILE"
        fi
    done

    # Inject Metadata
    cp "control.$COMP" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_I[@]}" "s|^Version:.*|Version: $VERSION|" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_I[@]}" "s|^Architecture:.*|Architecture: $ARCH|" "$STAGING_DIR/DEBIAN/control"
    
    for script in postinst prerm postrm; do
        if [ -f "$script" ]; then
            cp "$script" "$STAGING_DIR/DEBIAN/$script"
            chmod 755 "$STAGING_DIR/DEBIAN/$script"
        fi
    done
    

    echo "Generating conffiles for $COMP..."
    find "$STAGING_DIR/etc/starrocks/$COMP" -type f 2>/dev/null | sed "s|^$STAGING_DIR||" | LC_ALL=C sort > "$STAGING_DIR/DEBIAN/conffiles" || true  

    # Inject Systemd
    if [ -f "systemd/starrocks-$COMP.service" ]; then
        cp "systemd/starrocks-$COMP.service" "$STAGING_DIR/lib/systemd/system/"
    fi

    # Build .deb
    echo "Building $PACKAGE_NAME.deb..."
    dpkg-deb --root-owner-group --build "$STAGING_DIR" "target/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
done

echo "Build Complete! Files in target/"