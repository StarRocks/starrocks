# this file is used to execute as 
# sh gen_minidump_symbols.sh
# or
# ./gen_minidump_symbols.sh

set -e

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
starrocks_home=$(dirname $(dirname $curdir))

# generate symbol's file in Breakpad's own format.
$(dirname $curdir)/src/breakpad-main/src/tools/linux/dump_syms/dump_syms $starrocks_home/output/be/lib/starrocks_be > $starrocks_home/output/be/starrocks_be.sym

# remove debugging infos
strip $starrocks_home/output/be/lib/starrocks_be

# remove unneed old symbols
rm -rf $starrocks_home/output/be/symbols

# get last two strings as directory's name
a=$(head -n1 $starrocks_home/output/be/starrocks_be.sym)
array=(${a/// })

# create symbols' directory and move symbol file into it
mkdir -p $starrocks_home/output/be/symbols/${array[-1]}/${array[-2]}
mv $starrocks_home/output/be/starrocks_be.sym $starrocks_home/output/be/symbols/${array[-1]}/${array[-2]}

# echo symbol file's directory.
echo "symbol file is at" "$starrocks_home/output/be/symbols/${array[-1]}/${array[-2]}"

