#!/bin/bash

tool_opts=()

language_version=""
language_name=""
tool_name=""
tool_version=""
command=""
language_version_flag=0
language_name_flag=0
tool_name_flag=0
tool_version_flag=0
command_flag=0
for var in $@
do

    if [ $language_version_flag -eq 1 ]
    then
        # we have the python version
        language_version=$var
        language_version_flag=0
    elif [ $tool_name_flag -eq 1 ]
    then
        # we have the tool name
        tool_name=$var
        tool_name_flag=0
    elif [ $tool_version_flag -eq 1 ]
    then
        # we have the tool version
        tool_version=$var
        tool_version_flag=0
    elif [ $language_name_flag -eq 1 ]
    then
        # we have the language name
        language_name=$var
        language_name_flag=0
    elif [ $command_flag -eq 1 ]
    then
        # we have the command
        command=$var
        command_flag=0
    elif [ "$var" == "--language_version" ]
    then
        # we are going to get the language version
        language_version_flag=1
    elif [ "$var" == "--language" ]
    then
        language_name_flag=1
    elif [ "$var" == "--version" ]
    then
        # we are going to get the tool version
        tool_version_flag=1
    elif [ "$var" == "--tool" ]
    then
        # we are going to get the tool name
        tool_name_flag=1
    elif [ "$var" == "--cmd" ]
    then
        # we are going to get a command
        command_flag=1
    else
        # we're handling tool options
        tool_opts+=("$var")
    fi
done

usage()
{
cat << EOF
Usage:     tool.sh <tool> <version> <language> <language_version> <command> [options]

Example:   tool.sh --tool "remove-variants" --version "0.1.1" --language "python" --language_version "default" --cmd --help 
EOF
}

# ::TODO:: This causes cmo_facets to return exitcode 0 always, but we need to fix FACETS itself.
export FACETS_OVERRIDE_EXITCODE="set"

if [ -z "$language_name" ]
then
    echo "No language name defined"
    usage
    exit 1
fi
if [ -z "$language_version" ]
then
    echo "No language version defined"
    usage
    exit 1
fi
if [ -z "$tool_name" ]
then
    echo "No tool defined"
    usage
    exit 1
fi
if [ -z "$tool_version" ]
then
    echo "No tool version defined"
    usage
    exit 1
fi
language_path="cat $CMO_RESOURCE_CONFIG | jq '.[\"programs\"][\"${language_name}\"][\"${language_version}\"]'"
tool_path="cat $CMO_RESOURCE_CONFIG | jq '.[\"programs\"][\"${tool_name}\"][\"${tool_version}\"]'"
# Get the tool and python command from the resource.json and also allow spaces in the commands
language_cmd=$(eval $language_path)
language_cmd="${language_cmd:1:${#language_cmd}-2}"
tool_cmd=$(eval $tool_path)
tool_cmd="${tool_cmd:1:${#tool_cmd}-2}"
eval "$language_cmd ${tool_cmd} ${command} ${tool_opts[*]}"
