#!/bin/bash

usage()
{
cat << EOF

USAGE: `basename $0` [options]

OPTIONS:

   -v      Pipeline name/version
   -u      Username you want to configure a workplace for
   -f      Overwrite workspace even if it already exists
   -z      Display installed pipeline name/version

EXAMPLE:

   `basename $0` -v roslin-variant/2.5.0 -u nikhil

EOF
}

force_overwrite=0

while getopts “v:u:sfzh” OPTION
do
    case $OPTION in
        v) pipeline_name_version=$OPTARG ;;
        u) user_id=$OPTARG ;;
        s) use_single_machine_example=1 ;;
        f) force_overwrite=1 ;;
        z) find $ROSLIN_CORE_CONFIG_PATH -mindepth 2 -type d | grep -o -P "/config/.*" | cut -c9- | sort; exit 1 ;;
        h) usage; exit 1 ;;
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

if [ -z $user_id ]
then
    usage
    exit 1
fi

if [ -d "${ROSLIN_PIPELINE_WORKSPACE_PATH}/${user_id}" ] && [ "${force_overwrite}" -eq 0 ]
then
    echo "Your workspace already exists: ${ROSLIN_PIPELINE_WORKSPACE_PATH}/${user_id}"
    echo "Aborted."
    exit 1
fi

# create user directory
mkdir -p ${ROSLIN_PIPELINE_WORKSPACE_PATH}/${user_id}