#!/bin/bash

# do not echo out anything,
# otherwise sing.sh ... | sing.sh ... won't work

if [ -z $ROSLIN_PIPELINE_BIN_PATH ] || [ -z $ROSLIN_PIPELINE_DATA_PATH ] || \
   [ -z $ROSLIN_PIPELINE_WORKSPACE_PATH ] || [ -z $ROSLIN_PIPELINE_OUTPUT_PATH ] || \
   [ -z "$ROSLIN_EXTRA_BIND_PATH" ] || [ -z $ROSLIN_SINGULARITY_PATH ]
then
    echo "Some of the necessary paths are not correctly configured!"
    echo "ROSLIN_PIPELINE_BIN_PATH=${ROSLIN_PIPELINE_BIN_PATH}"
    echo "ROSLIN_PIPELINE_DATA_PATH=${ROSLIN_PIPELINE_DATA_PATH}"
    echo "ROSLIN_EXTRA_BIND_PATH=${ROSLIN_EXTRA_BIND_PATH}"
    echo "ROSLIN_PIPELINE_WORKSPACE_PATH=${ROSLIN_PIPELINE_WORKSPACE_PATH}"
    echo "ROSLIN_PIPELINE_OUTPUT_PATH=${ROSLIN_PIPELINE_OUTPUT_PATH}"
    echo "ROSLIN_SINGULARITY_PATH=${ROSLIN_SINGULARITY_PATH}"
    exit 1
fi

usage()
{
cat << EOF

Usage:     sing.sh <tool-name> <tool-version> [options]

Example:   sing.sh samtools 1.3.1 view sample.bam

EOF
}

# path to container images
container_image_path="${ROSLIN_PIPELINE_BIN_PATH}/img"

tool_name=${@:$OPTIND:1}
tool_version=${@:$OPTIND+1:1}

if [ -z "$tool_name" ] || [ -z "$tool_version" ];
then
  usage; exit 1;
fi

shift
shift

# start a singularity container with an empty environment
${ROSLIN_SINGULARITY_PATH} run \
  --cleanenv \
  ${container_image_path}/${tool_name}/${tool_version}/${tool_name}.sif "$1" ${@:2}
