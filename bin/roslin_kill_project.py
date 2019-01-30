#!/usr/bin/env python
from __future__ import print_function
import argparse
from core_utils import send_user_kill_signal


def main():
    "main function"

    parser = argparse.ArgumentParser(description='roslin kill project', add_help=False)

    parser.add_argument(
        "--name",
        action="store",
        dest="pipeline_name",
        help="Pipeline name (e.g. variant)",
        required=True
    )

    parser.add_argument(
        "--version",
        action="store",
        dest="pipeline_version",
        help="Pipeline version (e.g. 2.4.0)",
        required=True
    )

    parser.add_argument(
        "--id",
        action="store",
        dest="project_id",
        help="Project ID (e.g. Proj_5088_B)",
        required=True
    )

    parser.add_argument(
        "--uuid",
        action="store",
        dest="project_uuid",
        help="The uuid of the project",
        required=True
    )

    parser.add_argument(
        "--forceful",
        action="store_true",
        dest="forceful",
        help="Forcefully kill the project"
    )



    options = parser.parse_args()
    pipeline_name = options.pipeline_name
    pipeline_version = options.pipeline_version
    project_id = options.project_id
    project_uuid = options.project_uuid
    forceful = options.forceful
    termination_graceful = True
    if forceful:
        termination_graceful = False
    send_user_kill_signal(project_id, project_uuid, pipeline_name, pipeline_version, termination_graceful)

if __name__ == "__main__":

    main()
