#!/usr/bin/bash

coredump_log()
{
    echo "[`date`] [coredump] $@" >&2
}

# Define constant for 1TB in bytes
TB_IN_BYTES=$((1024 * 1024 * 1024 * 1024))

COREDUMP_PATH=$STARROCKS_HOME/storage/coredumps

if [ ! -d ${COREDUMP_PATH} ]; then
    mkdir -p ${COREDUMP_PATH}
fi

cd $COREDUMP_PATH

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
    coredump_log "missiong core file name"

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


  DATESTR=`date +"%m-%d-%y.%H-%M-%S.%Z"`
  COREDUMP_FILE_ZIP=${KUBE_CLUSTER_NAME}.${POD_NAMESPACE}.${POD_NAME}.${SR_IMAGE_TAG}.${DATESTR}.gz

  coredump_log "Compressing core dump files to ${COREDUMP_FILE_ZIP} ..."
  pigz -c $latestCoreFile > $COREDUMP_FILE_ZIP


  coredump_log "Zip complete"
  coredump_log "$(ls -lh ${COREDUMP_FILE_ZIP})"


  rclone --config=/opt/starrocks/rclone.conf --bwlimit 1000M --multi-thread-streams 100 --multi-thread-cutoff 8M --progress sync $COREDUMP_FILE_ZIP coredump:${COREDUMP_BLOBSTORE_PREFIX}/${KUBE_CLUSTER_NAME}/${POD_NAMESPACE}


  coredump_log "Upload complete: ${latestCoreFile}"

  # Only clean the uploaded core dump file
  rm $latestCoreFile $COREDUMP_FILE_ZIP

done
