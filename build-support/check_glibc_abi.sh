#!/usr/bin/env bash
# Fail the build when a binary references glibc symbol versions that the oldest
# supported runtime cannot provide.
#
# The dev-env image's glibc decides which symbol versions the linker stamps onto
# our references, so building in a newer image silently produces binaries that
# will not even load on an older host ("version `GLIBC_2.38' not found").
#
#   check_glibc_abi.sh <file-or-dir>...        # floors: $GLIBC_ABI_MAX   (default 2.35)
#                                             #         $GLIBCXX_ABI_MAX (default 3.4.30)
#                                             #         $CXXABI_ABI_MAX  (default 1.3.13)
#
# GLIBCXX_/CXXABI_ are checked for the same reason: a newer libstdc++ in the build
# image would be the same trap. BE links libstdc++ statically today, so those
# reference sets are empty - the check exists so a switch to dynamic linking cannot
# reintroduce the problem unnoticed.
#
# be/src/common/glibc_compat.c exists to keep this check passing; when it starts
# failing, extend that file rather than lowering the floor.
set -uo pipefail

FLOOR="${GLIBC_ABI_MAX:-2.35}"
CXX_FLOOR="${GLIBCXX_ABI_MAX:-3.4.30}"
# CXXABI_ is versioned independently of GLIBCXX_ (1.3.x vs 3.4.x); comparing it
# against the GLIBCXX floor would accept every CXXABI reference unconditionally.
ABI_FLOOR="${CXXABI_ABI_MAX:-1.3.13}"
[[ $# -gt 0 ]] || { echo "usage: $(basename "$0") <file-or-dir>..." >&2; exit 2; }

# 2.35 -> 2035000, 2.4 -> 2004000  (so plain string compare orders correctly)
ver_key() { awk -F. '{printf "%d%03d%03d\n", $1, $2, ($3 == "" ? 0 : $3)}' <<<"$1"; }
FLOOR_KEY=$(ver_key "$FLOOR")
CXX_FLOOR_KEY=$(ver_key "$CXX_FLOOR")
ABI_FLOOR_KEY=$(ver_key "$ABI_FLOOR")

mapfile -t files < <(
  for p in "$@"; do
    if [[ -d $p ]]; then find "$p" -type f \( -name '*.so' -o -name '*.so.*' -o -perm -u+x \)
    else echo "$p"
    fi
  done | sort -u
)

rc=0
checked=0
for f in "${files[@]}"; do
  [[ $(head -c4 "$f" 2>/dev/null) == $'\x7fELF' ]] || continue
  checked=$((checked + 1))
  bad=$(nm -D --undefined-only "$f" 2>/dev/null |
        grep -oE '[^ ]+@(GLIBC|GLIBCXX|CXXABI)_[0-9.]+' | sort -u | while IFS= read -r ref; do
    v=${ref##*_}
    case $ref in
      *@GLIBC_*)   [[ $(ver_key "$v") -gt $FLOOR_KEY ]]     && echo "    $ref" ;;
      *@GLIBCXX_*) [[ $(ver_key "$v") -gt $CXX_FLOOR_KEY ]] && echo "    $ref" ;;
      *@CXXABI_*)  [[ $(ver_key "$v") -gt $ABI_FLOOR_KEY ]] && echo "    $ref" ;;
    esac
  done)
  if [[ -n $bad ]]; then
    echo "glibc ABI check FAILED: $f needs symbol versions newer than GLIBC_$FLOOR / GLIBCXX_$CXX_FLOOR / CXXABI_$ABI_FLOOR:" >&2
    echo "$bad" >&2
    rc=1
  fi
done

if [[ $rc -eq 0 ]]; then
  echo "glibc ABI check passed: $checked ELF file(s) stay within GLIBC_$FLOOR / GLIBCXX_$CXX_FLOOR / CXXABI_$ABI_FLOOR."
fi
exit $rc
