#!/usr/bin/bash

coredump_log()
{
    echo "[`date`] [coredump] $@" >&2
}

STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}

# Define constant for 1TB in bytes
TB_IN_BYTES=$((1024 * 1024 * 1024 * 1024))

# Set COREDUMP_PATH if hasn't been set in environment variable
COREDUMP_PATH=${COREDUMP_PATH:-$STARROCKS_HOME/storage/coredumps}

# Default minimal interval in 600 seconds
COREDUMP_COLLECT_MINIMUM_INTERVAL=${COREDUMP_COLLECT_MINIMUM_INTERVAL:-600}


if [ ! -d ${COREDUMP_PATH} ]; then
    mkdir -p ${COREDUMP_PATH}
fi

# log all the core dump setup
COREDUMP_VARIABLES=$(declare -p |awk '$3 ~ /^COREDUMP_/ {print $3}')
coredump_log $COREDUMP_VARIABLES


cd $COREDUMP_PATH

previousFileTimestamp=0

while true; do

  latestCoreFile=""

  # Setup inotifywait loop to wait until core file has been completely written
  while read -r path action file; do
    coredump_log "${action} ${path}${file}"

    if [[ "$file" = core.*  ]]; then
      latestCoreFile=$file
    fi
  done < <(inotifywait -e close_write $COREDUMP_PATH)

  if [[ $latestCoreFile == "" ]]; then
    coredump_log "missing core file name"

    continue
  fi

  coredump_log "Core dump generated $latestCoreFile"
  coredump_log "$(ls -lh ${latestCoreFile})"

  fileSizeBytes=$(stat -c %s "${latestCoreFile}")

  # Check if file size is greater than 1TB
  if (( fileSizeBytes > TB_IN_BYTES )); then
      coredump_log "File size (${fileSizeBytes} B) exceeds 1TB: ${fileSizeBytes} bytes, skip"
      rm "${latestCoreFile}"
      continue
  fi

  # Check if the creation time of the file is more than 10 minutes later than the file handles in the previous loop iteration
  currentFileTimestamp=$(stat -c %Y "${latestCoreFile}")
  timeDifference=$((currentFileTimestamp - previousFileTimestamp))

  if (( timeDifference < $COREDUMP_COLLECT_MINIMUM_INTERVAL )); then
    coredump_log "File was created less than ${COREDUMP_COLLECT_MINIMUM_INTERVAL} seconds after the previous crash, removing file and continuing loop"
    rm "${latestCoreFile}"
    continue
  fi

  previousFileTimestamp=$currentFileTimestamp

  DATESTR=`date +"%m-%d-%y.%H-%M-%S.%Z"`
  COREDUMP_FILE_ZIP=${KUBE_CLUSTER_NAME}.${POD_NAMESPACE}.${POD_NAME}.${SR_IMAGE_TAG}.${DATESTR}.gz

  coredump_log "Compressing core dump files to ${COREDUMP_FILE_ZIP} ..."
  pigz -c $latestCoreFile > $COREDUMP_FILE_ZIP


  coredump_log "Zip complete"
  coredump_log "$(ls -lh ${COREDUMP_FILE_ZIP})"


  rclone --config=$STARROCKS_ROOT/rclone.conf --bwlimit 1000M --multi-thread-streams 100 --multi-thread-cutoff 8M --progress sync $COREDUMP_FILE_ZIP coredump:${COREDUMP_BLOBSTORE_PREFIX}/${KUBE_CLUSTER_NAME}/${POD_NAMESPACE}


  coredump_log "Upload complete: ${latestCoreFile}"

  # Only clean the core dump files
  # Note: it's intentially avoid using * for all files to minimize the accidentialy deletion of other useful files
  rm $COREDUMP_PATH/core.* &&  rm $COREDUMP_PATH/*.gz

done
