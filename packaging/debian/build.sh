#!/usr/bin/env bash
set -e

# --- StarRocks Debian Packaging Build Script ---
# Purpose: Generates .deb packages for FE, BE, and CN components.
# Usage: ./build.sh <version> [fe_src] [be_src] [arch] [cn_src]
# Example: ./build.sh 4.0.4 ../../out/fe ../../out/be amd64 ../../out/be
#
# Parameters:
# $1: Version (if omitted, will be read from ./VERSION if present)
# $2: FE Source directory (default: ../../out/fe)
# $3: BE Source directory (default: ../../out/be)
# $4: Architecture (default: output of dpkg --print-architecture)
# $5: CN Source directory (default: defaults to BE_SOURCE)

if [ -n "$1" ]; then
    VERSION="$1"
elif [ -f VERSION ]; then
    VERSION="$(cat VERSION)"
else
    echo "ERROR: Version not specified and VERSION file not found." >&2
    echo "Please rerun as: $0 <version> [fe_src] [be_src] [arch] [cn_src]" >&2
    exit 1
fi
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

# Validate that the version is not the placeholder
if [ "$VERSION" = "0.0.0" ]; then
    echo "ERROR: Version is set to 0.0.0. Please provide a real version number." >&2
    echo "Usage: $0 <version> [fe_src] [be_src] [arch] [cn_src]" >&2
    exit 1
fi

# Detect OS for portable sed
if [ -z "$BASH_VERSION" ]; then
    echo "ERROR: This script requires Bash." >&2
    exit 1
fi

# Check for required Debian packaging tools
for tool in dpkg-deb dpkg-query; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: Required tool '$tool' not found." >&2
        if [[ "$OSTYPE" == "darwin"* ]]; then
            echo "On macOS, you can install these via: brew install dpkg" >&2
        else
            echo "Please install the 'dpkg' or 'dpkg-dev' package for your distribution." >&2
        fi
        exit 1
    fi
done

# Check for minimum dpkg version (1.19.0) for --root-owner-group support
if command -v dpkg >/dev/null 2>&1; then
    DPKG_VERSION=$(dpkg-query -W -f='${Version}' dpkg | cut -d'~' -f1)
    
    # Use dpkg's built-in comparison tool: lt = "less than"
    if dpkg --compare-versions "$DPKG_VERSION" lt "1.19.0"; then
        echo "ERROR: dpkg version $DPKG_VERSION is too old." >&2
        echo "This script requires dpkg >= 1.19.0 for --root-owner-group support." >&2
        echo "Please build on Debian 10+, Ubuntu 18.10+, or a modern equivalent." >&2
        exit 1
    fi
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS requires an explicit empty string for the backup extension
    SED_INPLACE=(-i '')
else
    # Linux (GNU sed) does not
    SED_INPLACE=(-i)
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
        echo "ERROR: Source directory $SRC_DIR not found!" >&2
        exit 1
    fi

    # Validate required subdirectories and files before copying
    REQUIRED_PATHS=(
        "$SRC_DIR/bin"
        "$SRC_DIR/lib"
        "$SRC_DIR/conf"
        "$SRC_DIR/conf/$COMP.conf"
        "$SRC_DIR/bin/start_${COMP}.sh"
    )
    # Add config files to validation
    if [ "$COMP" == "be" ] || [ "$COMP" == "cn" ]; then
        REQUIRED_PATHS+=("$SRC_DIR/conf/be.conf" "$SRC_DIR/conf/cn.conf")
    fi

    for REQ in "${REQUIRED_PATHS[@]}"; do
        if [ ! -e "$REQ" ]; then
            echo "ERROR: Required path for component '$COMP' is missing: $REQ" >&2
            exit 1
        fi
    done

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
    
    # Ensure shell scripts and binaries have appropriate execute permissions (755)
    if [ -d "$STAGING_DIR/usr/lib/starrocks/$COMP/bin" ]; then
        find "$STAGING_DIR/usr/lib/starrocks/$COMP/bin" -type f -exec chmod 755 {} +
    fi
    
    if [ "$COMP" == "fe" ]; then
        [ -d "$SRC_DIR/spark-dpp" ] && cp -a "$SRC_DIR/spark-dpp" "$STAGING_DIR/usr/lib/starrocks/fe/"
        [ -d "$SRC_DIR/webroot" ] && cp -a "$SRC_DIR/webroot" "$STAGING_DIR/usr/lib/starrocks/fe/"
    else
        [ -d "$SRC_DIR/www" ] && cp -a "$SRC_DIR/www" "$STAGING_DIR/usr/lib/starrocks/$COMP/"
    fi

    # Copy Configs and set up symlink
    cp -a "$SRC_DIR/conf/"* "$STAGING_DIR/etc/starrocks/$COMP/"
    ln -sfn "/etc/starrocks/$COMP" "$STAGING_DIR/usr/lib/starrocks/$COMP/conf"

    # Cleanup either BE or CN
    if [ "$COMP" == "be" ]; then
        rm -f "$STAGING_DIR/etc/starrocks/be/cn.conf"
        mkdir -p "$STAGING_DIR/usr/share/doc/starrocks-be"  
        echo "Note: BE and CN are mutually exclusive. Do not convert this BE directory to CN in-place." > "$STAGING_DIR/usr/share/doc/starrocks-be/PACKAGE_NOTE"  
    elif [ "$COMP" == "cn" ]; then  
        rm -f "$STAGING_DIR/etc/starrocks/cn/be.conf"  
        mkdir -p "$STAGING_DIR/usr/share/doc/starrocks-cn"  
        echo "Note: CN and BE are mutually exclusive. Do not convert this CN directory to BE in-place." > "$STAGING_DIR/usr/share/doc/starrocks-cn/PACKAGE_NOTE"  
    fi

    # Path Patching (LSB)
    echo "Patching $COMP.conf for standard paths..."
    CONF_FILE="$STAGING_DIR/etc/starrocks/$COMP/$COMP.conf"
    sed "${SED_INPLACE[@]}" 's/\r$//' "$CONF_FILE"

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
        sed "${SED_INPLACE[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$KEY[[:space:]]*=[[:space:]]*\$\{STARROCKS_HOME\}.*|$KEY = $VAL|" "$CONF_FILE"
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
            sed "${SED_INPLACE[@]}" -E "s|^[[:space:]]*#?[[:space:]]*$VAR[[:space:]]*=.*|$VAR = $VALUE|" "$CONF_FILE"
        else
            echo "Appending $VAR to $CONF_FILE"
            echo "$VAR = $VALUE" >> "$CONF_FILE"
        fi
    done

    # Inject Metadata
    cp "control.$COMP" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_INPLACE[@]}" "s|^Version:.*|Version: $VERSION|" "$STAGING_DIR/DEBIAN/control"
    sed "${SED_INPLACE[@]}" "s|^Architecture:.*|Architecture: $ARCH|" "$STAGING_DIR/DEBIAN/control"
    
    for script in postinst prerm postrm; do
        if [ -f "$script" ]; then
            cp "$script" "$STAGING_DIR/DEBIAN/$script"
            chmod 755 "$STAGING_DIR/DEBIAN/$script"
        fi
    done
    

    echo "Generating conffiles for $COMP..."
    if [ -d "$STAGING_DIR/etc/starrocks/$COMP" ]; then
        if ! find "$STAGING_DIR/etc/starrocks/$COMP" -type f | sed "s|^$STAGING_DIR||" | LC_ALL=C sort > "$STAGING_DIR/DEBIAN/conffiles"; then
            echo "ERROR: Failed to generate conffiles for $COMP" >&2
            exit 1
        fi
    else
        echo "No configuration directory found for $COMP at $STAGING_DIR/etc/starrocks/$COMP; generating empty conffiles."
        : > "$STAGING_DIR/DEBIAN/conffiles"
    fi

    # Inject Systemd
    if [ -f "systemd/starrocks-$COMP.service" ]; then
        cp "systemd/starrocks-$COMP.service" "$STAGING_DIR/lib/systemd/system/"
    fi

    # Build .deb
    echo "Building $PACKAGE_NAME.deb..."
    # NOTE: --root-owner-group requires dpkg >= 1.19.0 (Debian 10+, Ubuntu 18.10+).
    # We intentionally do not support legacy versions as StarRocks requires a 
    # modern environment (GLIBC, etc.) found in these versions or newer.
    dpkg-deb --root-owner-group --build "$STAGING_DIR" "target/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
done

echo "Build Complete! Files in target/"