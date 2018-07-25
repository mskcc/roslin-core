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

# set up singularity bind paths
bind_path=""
for single_bind_path in ${ROSLIN_BIND_PATH}
do
  bind_path="${bind_path} --bind ${single_bind_path}:${single_bind_path}"
done

# path to container images
container_image_path="${ROSLIN_PIPELINE_BIN_PATH}/img"

while getopts “i” OPTION
do
    case $OPTION in
        i) inspect="set" ;;
    esac
done

tool_name=${@:$OPTIND:1}
tool_version=${@:$OPTIND+1:1}

if [ -z "$tool_name" ] || [ -z "$tool_version" ];
then
  usage; exit 1;
fi

shift
shift

# output metadata (labels) if the inspect option (-i) is supplied
if [ "$inspect" = "set" ]
then
${ROSLIN_SINGULARITY_PATH} exec \
    --cleanenv \
    ${container_image_path}/${tool_name}/${tool_version}/${tool_name}.sqsh \
    cat /.roslin/labels.json
  exit $?
fi

# start a singularity container with an empty environment
${ROSLIN_SINGULARITY_PATH} run \
  --cleanenv \
  ${bind_path} \
  ${container_image_path}/${tool_name}/${tool_version}/${tool_name}.sqsh $*
