#!/usr/bin/env python3
import os, sys
import logging
import argparse
import json
if 'ROSLIN_CORE_BIN_PATH' not in os.environ:
    print("Roslin core settings not loaded")
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
ROSLIN_CORE_CONFIG_PATH = os.environ['ROSLIN_CORE_CONFIG_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import run_command_realtime, print_error
from roslin_submit import  get_pipeline_name_and_versions

logger = logging.getLogger("roslin_workspace_init")

def main():
    "main function"

    preparser = argparse.ArgumentParser(description='roslin workspace init', add_help=False)

    pipeline_name_and_version = get_pipeline_name_and_versions()


    preparser.add_argument(
        "--name",
        action="store",
        dest="pipeline_name",
        help=pipeline_name_and_version['pipeline_name_help'],
        choices=pipeline_name_and_version['pipeline_name_choices'],
        required=True
    )

    preparser.add_argument(
        "--version",
        action="store",
        dest="pipeline_version",
        help=pipeline_name_and_version['pipeline_version_help'],
        choices=pipeline_name_and_version['pipeline_version_choices'],
        required=True
    )

    name_and_version, _ = preparser.parse_known_args()
    pipeline_name = name_and_version.pipeline_name
    pipeline_version = name_and_version.pipeline_version
    # load the Roslin Pipeline settings
    pipeline_name_version = os.path.join(pipeline_name,pipeline_version)
    settings_path = os.path.join(os.environ.get("ROSLIN_CORE_CONFIG_PATH"), pipeline_name_version, "settings.sh")
    workspace_init_command = ['bash','-c',settings_path]
    command_output = run_command_realtime(workspace_init_command,False)
    if command_output['errorcode'] != 0:
        error = command_output['error']
        print_error("Workspace creation failed")
        if error != '':
            print_error("Error:\n" + error)
        exit(1)


if __name__ == "__main__":

    main()
