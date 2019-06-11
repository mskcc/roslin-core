#!/bin/bash

if [ -z "$ROSLIN_CORE_VERSION" ] || [ -z "$ROSLIN_CORE_ROOT" ] || \
   [ -z "$ROSLIN_CORE_PATH" ] || [ -z "$ROSLIN_CORE_BIN_PATH" ] || \
   [ -z "$ROSLIN_CORE_CONFIG_PATH" ]
then
    echo "Some of the Roslin Core settings are not found."
    echo "ROSLIN_CORE_VERSION=${ROSLIN_CORE_VERSION}"
    echo "ROSLIN_CORE_ROOT=${ROSLIN_CORE_ROOT}"
    echo "ROSLIN_CORE_PATH=${ROSLIN_CORE_PATH}"
    echo "ROSLIN_CORE_BIN_PATH=${ROSLIN_CORE_BIN_PATH}"
    echo "ROSLIN_CORE_CONFIG_PATH=${ROSLIN_CORE_CONFIG_PATH}"
    exit 1
fi

# set defaults
pipeline_name_version=${ROSLIN_DEFAULT_PIPELINE_NAME_VERSION}
debug_options=""
restart_options=""
mem_options=""
cores_options=""
restart_jobstore_id=""
batch_system=""
output_directory="./outputs"

usage()
{
cat << EOF

USAGE: `basename $0` [options]

Roslin Runner

OPTIONS:

   -v      Pipeline name/version (default=${pipeline_name_version})
   -w      Workflow filename (*.cwl)
   -i      Input filename (*.yaml)
   -b      Batch system (e.g. "singleMachine", "lsf", "mesos")
   -o      Output directory (default=${output_directory})
   -j      Jobstore UUID
   -u      Job UUID
   -k      Toil work directory location
   -m      Max Mem (e.g. 256G)
   -c      Max Cores (e.g. 14)
   -r      Restart the workflow with the given job store UUID
   -t      Run pipeline in test mode
   -z      Show list of supported workflows
   -d      Enable debugging

EXAMPLE:

   `basename $0` -v variant/2.5.0 -w module-1.cwl -i inputs.yaml -k /scratch -j [uuid1] -u [uuid2] -b lsf
   `basename $0` -v rna-seq/1.0.0 -w hisat2.cwl -i inputs.yaml -k /scratch -j [uuid1] -u [uuid2] -b singleMachine

EOF
}

while getopts “v:w:i:b:o:j:k:m:c:rtzd:u:” OPTION
do
    case $OPTION in
        v) pipeline_name_version=$OPTARG ;;
        w) workflow_filename=$OPTARG ;;
        i) input_filename=$OPTARG ;;
        b) batch_system=$OPTARG ;;
        o) output_directory=$OPTARG ;;
        j) JOBSTORE_ID=$OPTARG ;;
        k) work_dir=$OPTARG ;;
        m) max_mem=$OPTARG ;;
        c) max_cores=$OPTARG ;;
        r) restart_options="--restart" ;;
        t) test_mode="True" ;;
        z) cd ${ROSLIN_PIPELINE_CWL_PATH}
           find . -name "*.cwl" -exec bash -c "echo {} | cut -c 3- | sort" \;
           exit 0
           ;;
        d) debug_options="--logDebug --cleanWorkDir never" ;;
        u) JOB_UUID=$OPTARG ;;
        *) usage; exit 1 ;;
    esac
done

if [ -z "$pipeline_name_version" ]
then
    usage
    exit 1
fi

if [ ! -r "${ROSLIN_CORE_CONFIG_PATH}/${pipeline_name_version}/settings.sh" ]
then
    echo "Can't find/read the specified Pipeline name/version."
    echo "${ROSLIN_CORE_CONFIG_PATH}/${pipeline_name_version}/settings.sh"
    exit 1
fi

pipeline_cwl_path=${ROSLIN_PIPELINE_CWL_PATH}
# load pipeline settings
source ${ROSLIN_CORE_CONFIG_PATH}/${pipeline_name_version}/settings.sh

if [ ! -z $test_mode ]
then
    echo "Running in test mode"
    source ${ROSLIN_CORE_CONFIG_PATH}/${pipeline_name_version}/test-settings.sh

fi

if [ ! -z $max_mem ]
then
  mem_options="--maxMemory $max_mem"
fi

if [ ! -z $max_cores ]
then
  cores_options="--maxCores $max_cores"
fi

if [ -z "$ROSLIN_PIPELINE_BIN_PATH" ] || [ -z "$ROSLIN_PIPELINE_DATA_PATH" ] || \
   [ -z "$ROSLIN_PIPELINE_WORKSPACE_PATH" ] || [ -z "$ROSLIN_PIPELINE_OUTPUT_PATH" ] || \
   [ -z "$ROSLIN_EXTRA_BIND_PATH" ] || [ -z "$ROSLIN_SINGULARITY_PATH" ] || \
   [ -z "$ROSLIN_CMO_VERSION" ] || [ -z "$ROSLIN_CMO_INSTALL_PATH" ] || [ -z "$ROSLIN_TOIL_INSTALL_PATH" ]
then
    echo "Some of the Roslin Pipeline settings are not found."
    echo "ROSLIN_PIPELINE_BIN_PATH=${ROSLIN_PIPELINE_BIN_PATH}"
    echo "ROSLIN_PIPELINE_DATA_PATH=${ROSLIN_PIPELINE_DATA_PATH}"
    echo "ROSLIN_EXTRA_BIND_PATH=${ROSLIN_EXTRA_BIND_PATH}"
    echo "ROSLIN_PIPELINE_WORKSPACE_PATH=${ROSLIN_PIPELINE_WORKSPACE_PATH}"
    echo "ROSLIN_PIPELINE_OUTPUT_PATH=${ROSLIN_PIPELINE_OUTPUT_PATH}"
    echo "ROSLIN_SINGULARITY_PATH=${ROSLIN_SINGULARITY_PATH}"
    echo "ROSLIN_CMO_VERSION=${ROSLIN_CMO_VERSION}"
    echo "ROSLIN_CMO_INSTALL_PATH=${ROSLIN_CMO_INSTALL_PATH}"
    echo "ROSLIN_TOIL_INSTALL_PATH=${ROSLIN_TOIL_INSTALL_PATH}"
    exit 1
fi

if [ ! -d $ROSLIN_CMO_PYTHON_PATH ]
then
    echo "Can't find python package at $ROSLIN_CMO_PYTHON_PATH"
    exit 1
fi

if [ -z $workflow_filename ] || [ -z $input_filename ]
then
    usage
    exit 1
fi

if [ ! -r $input_filename ]
then
    echo "The input file is not found or not readable."
    exit 1
fi

# handle batch system options
batch_sys_options="--batchSystem ${batch_system}"

# get absolute path for output directory
output_directory=`python -c "import os;print(os.path.abspath('${output_directory}'))"`

# create output directory
mkdir -p ${output_directory}

# create work directory
mkdir -p ${work_dir}

# create log directory (under output)
mkdir -p ${output_directory}/log

# override CMO_RESOURC_CONFIG only while cwltoil is running
export CMO_RESOURCE_CONFIG="${ROSLIN_PIPELINE_BIN_PATH}/scripts/roslin_resources.json"

if [ -z "${JOB_UUID}" ]
then
    # create a new UUID for job
    job_uuid=`python -c 'import uuid; print str(uuid.uuid1())'`
else
    # use the supplied one
    job_uuid=${JOB_UUID}
fi

if [ -z "${JOBSTORE_ID}" ]
then
    # create a new UUID for job
    job_store_uuid=`python -c 'import uuid; print str(uuid.uuid1())'`
else
    # use the supplied one
    job_store_uuid=${JOBSTORE_ID}
fi

# LSF+TOIL
export TOIL_LSF_ARGS="${TOIL_LSF_ARGS} -P ${job_uuid}"
job_store_uuid=${JOBSTORE_ID}

# save job uuid
echo "${job_uuid}" > ${output_directory}/job-uuid

# save jobstore uuid
echo "${job_store_uuid}" > ${output_directory}/job-store-uuid

# save the Roslin Pipeline settings.sh
cp ${ROSLIN_CORE_CONFIG_PATH}/${pipeline_name_version}/settings.sh ${output_directory}/settings

jobstore_path="${ROSLIN_PIPELINE_BIN_PATH}/tmp/${job_store_uuid}"
echo "${jobstore_path}"

# job uuid followed by a colon (:) and then job store uuid
printf "\n---> ROSLIN JOB UUID = ${job_uuid}:${job_store_uuid}\n"

echo "VERSIONS: roslin-core-${ROSLIN_CORE_VERSION}, roslin-${ROSLIN_PIPELINE_NAME}-${ROSLIN_PIPELINE_VERSION}, cmo-${ROSLIN_CMO_VERSION}"
# add virtualenv and sing to PATH
export PATH=$ROSLIN_CORE_CONFIG_PATH/$ROSLIN_PIPELINE_NAME/$ROSLIN_PIPELINE_VERSION/virtualenv/bin/:${ROSLIN_CORE_BIN_PATH}/sing:$PATH

# run cwltoil
set -o pipefail
cwltoil \
    ${restart_options} \
    --jobStore file://${jobstore_path} \
    --retryCount 1 \
    ${mem_options} \
    ${cores_options} \
    --preserve-environment PATH PYTHONPATH ROSLIN_PIPELINE_DATA_PATH ROSLIN_PIPELINE_BIN_PATH ROSLIN_EXTRA_BIND_PATH SINGULARITY_BIND ROSLIN_PIPELINE_WORKSPACE_PATH ROSLIN_PIPELINE_OUTPUT_PATH ROSLIN_SINGULARITY_PATH CMO_RESOURCE_CONFIG ROSLIN_MONGO_HOST ROSLIN_MONGO_PORT ROSLIN_MONGO_DATABASE ROSLIN_MONGO_USERNAME ROSLIN_MONGO_PASSWORD TMP TMPDIR ROSLIN_USE_DOCKER DOCKER_REGISTRY_NAME DOCKER_BIND ROSLIN_PIPELINE_CWL_PATH \
    --no-container \
    --not-strict \
    --disableCaching \
    --disableChaining \
    --realTimeLogging \
    --stats \
    --maxLogFileSize 0 \
    --writeLogs ${output_directory}/log \
    --logFile ${output_directory}/log/cwltoil.log \
    --tmpdir-prefix ${work_dir} \
    --workDir ${work_dir} \
    --outdir ${output_directory} ${batch_sys_options} ${debug_options} \
    ${pipeline_cwl_path}/${workflow_filename} \
    ${input_filename} \
    | tee ${output_directory}/output-meta.json
exit_code=$?

# revert CMO_RESOURCE_CONFIG
unset CMO_RESOURCE_CONFIG

# revert TOIL_LSF_PROJECT
unset TOIL_LSF_PROJECT

printf "\n<--- ROSLIN JOB UUID = ${job_uuid}:${job_store_uuid}\n\n"

exit ${exit_code}
