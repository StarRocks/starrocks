#!/usr/bin/bash

log_stdin()
{
    echo "[coredump] $@" >&1
}


cd $COREDUMP_PATH

while true; do

  latestCoreFile=""

  # Setup inotifywait loop to wait until core file has been completely written
  while read -r path action file; do
    log_stdin "${action} ${path}${file}"

    if [[ "$file" = core.*  ]]; then
      latestCoreFile=$file
    fi
  done < <(inotifywait -e close_write $COREDUMP_PATH)

  if [[ $latestCoreFile == "" ]]; then
    log_stdin "missiong core file name"

    continue
  fi

  log_stdin "Core dump generated $latestCoreFile"
  log_stdin "$(ls -lh ${latestCoreFile})"

  fileSize=$(ls -lh "${latestCoreFile}" | awk '{print $5}')
  fileSizeBytes=$(stat -c %s "${latestCoreFile}")


  # Check if file size is greater than 1TB (1000 GB)
  if [[ "$fileSize" =~ T$ ]]; then
      log_stdin "File size (${fileSizeBytes} B) exceeds 1TB: ${fileSizeBytes} bytes, skip"
      rm ${latestCoreFile}

      continue
  fi


  DATESTR=`TZ=${TZ} date +"%m-%d-%y.%H-%M-%S.%Z"`
  COREDUMP_FILE_ZIP=${KUBE_CLUSTER_NAME}.${POD_NAMESPACE}.${POD_NAME}.${SR_IMAGE_TAG}.${DATESTR}.gz
  COREDUMP_S3_PATH=${COREDUMP_S3_BUCKET}/${KUBE_CLUSTER_NAME}/${POD_NAMESPACE}

  # Subtract 4 from the number of CPU cores
  p_value=$(( $(nproc) - 4 ))
  log_stdin "Compressing core dump files with $p_value cpus to ${COREDUMP_FILE_ZIP} ..."
  pigz -p $p_value -c $latestCoreFile > $COREDUMP_FILE_ZIP

  log_stdin "Zip complete"
  log_stdin "$(ls -lh ${COREDUMP_FILE_ZIP})"

  log_stdin "Uploading compressed core dump to $COREDUMP_S3_PATH/$COREDUMP_FILE_ZIP ..."
  /opt/starrocks/s3sync -concurrency 1 \
    -accesskey $COREDUMP_S3_ACCESS_KEY \
    -secretkey $COREDUMP_S3_SECRET_KEY \
    -region $COREDUMP_S3_REGION \
    -s3path $COREDUMP_S3_PATH \
    -localdir $COREDUMP_FILE_ZIP \
    -upload

  log_stdin "Upload complete: ${latestCoreFile}"

  # Only clean the uploaded core dump file
  rm $latestCoreFile $COREDUMP_FILE_ZIP

done
