#!/usr/bin/env python3
import os, sys
import logging
import argparse
import json
import signal
from roslin_submit import  JOBSTORE_UUID_FILE_NAME, INPUT_YAML_FILE_NAME, WORKFLOW_PARAMS_FILE_NAME, submit, get_pipeline_name_and_versions, get_input_metadata, targzip_project_files

if 'ROSLIN_CORE_BIN_PATH' not in os.environ:
    print("Roslin core settings not loaded")
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
ROSLIN_CORE_CONFIG_PATH = os.environ['ROSLIN_CORE_CONFIG_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import load_pipeline_settings, copy_ignore_same_file, convert_yaml_abs_path, run_command, run_command_realtime, print_error, send_user_kill_signal, check_if_argument_file_exists, check_yaml_boolean_value, load_yaml, save_yaml, check_tmp_env, get_common_args, parse_workflow_args, get_submission_args, add_specific_args, get_leader_args, get_args_dict, get_restart_args

logger = logging.getLogger("roslin_restart")

def cleanup(clean_up_tuple, signal_num, frame):
    signal_name = "Unknown"
    if signal_num == signal.SIGINT:
        signal_name = "SIGINT"
    if signal_num == signal.SIGTERM:
        signal_name = "SIGTERM"
    signal_message = "Received signal: "+ signal_name
    print(signal_message)
    send_user_kill_signal(*clean_up_tuple)


def main():
    "main function"

    preparser = argparse.ArgumentParser(description='roslin restart', add_help=False)

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
    pipeline_settings = load_pipeline_settings(pipeline_name, pipeline_version)
    if not pipeline_settings:
        print_error("Error "+ str(pipeline_name) + " version "+ str(pipeline_version) + " does not exist.")
        exit(1)
    sys.path.append(pipeline_settings['ROSLIN_PIPELINE_BIN_PATH'])


    parser = argparse.ArgumentParser(parents=[ preparser ], add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    restart_args_list = get_restart_args()
    parser = add_specific_args(parser,restart_args_list)
    params, _ = parser.parse_known_args()
    restart_job_uuid = params.restart_job_uuid
    work_base_dir = os.path.abspath(pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"])
    inputs_yaml = None
    jobstore_uuid = ''
    previous_work_base_dir = os.path.join(work_base_dir, restart_job_uuid[:8], restart_job_uuid)
    error_extra_info =  "for " + pipeline_name + " (" + pipeline_version + ") "
    if not os.path.exists(previous_work_base_dir):
        print_error("ERROR: Could not find project with uuid: " + restart_job_uuid + error_extra_info)
        exit(1)
    previous_jobstore_uuid_path = os.path.join(previous_work_base_dir,JOBSTORE_UUID_FILE_NAME)
    if not os.path.exists(previous_jobstore_uuid_path):
        print_error("ERROR: Could not find jobstore uuid file ( " + JOBSTORE_UUID_FILE_NAME +  " ) in "+previous_work_base_dir +"\n"+error_extra_info)
        exit(1)
    with open(previous_jobstore_uuid_path) as previous_jobstore_uuid_file:
        jobstore_uuid = previous_jobstore_uuid_file.readline().strip()
    job_uuid = restart_job_uuid
    restart = True
    project_path = None
    inputs_yaml = os.path.abspath(os.path.join(previous_work_base_dir,INPUT_YAML_FILE_NAME))
    workflow_params_path = os.path.abspath(os.path.join(previous_work_base_dir,'tmp',WORKFLOW_PARAMS_FILE_NAME))
    with open(workflow_params_path) as workflow_params_file:
        workflow_params_data = json.load(workflow_params_file)
    workflow_params_data['foreground_mode'] = False
    params_dict = params.__dict__
    for single_arg in params_dict:
        if params_dict[single_arg] != parser.get_default(single_arg):
            workflow_params_data[single_arg] = params_dict[single_arg]

    project_id = workflow_params_data['project_id']

    inputs_yaml_dirname = os.path.dirname(inputs_yaml)
    inputs_yaml_basename = os.path.basename(inputs_yaml)
    check_tmp_env(None)

    if workflow_params_data['force_overwrite_results'] and not workflow_params_data['results_dir']:
        print_error("ERROR: You need to specify an output directory to force overwrite")
        sys.exit(1)

    work_dir = os.path.join(work_base_dir, job_uuid[:8], job_uuid)
    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

#    if 'project_path' in workflow_params_data and workflow_params_data['project_path'] != None:
#        project_path = os.path.abspath(workflow_params_data['project_path'])
#    else:

    project_path = work_dir
    workflow_params_data['project_path'] = work_dir

    if project_path:
        input_metadata_filenames = get_input_metadata(project_path)
        for filename in input_metadata_filenames:
            input_metadata_location = os.path.join(project_path,filename)
            input_metadata_work_dir_location = os.path.join(work_dir,filename)
            if os.path.exists(input_metadata_location):
                copy_ignore_same_file(input_metadata_location,input_metadata_work_dir_location)
    else:
        input_files_blob = None

    jobstore_uuid_path = os.path.join(work_dir,JOBSTORE_UUID_FILE_NAME)
    input_yaml_path = os.path.join(work_dir,INPUT_YAML_FILE_NAME)

    inputs_yaml_work_dir = os.path.join(work_dir,inputs_yaml_basename)
    # convert any relative path in inputs.yaml (e.g. path: ../abc)
    # to absolute path (e.g. path: /ifs/abc)
    convert_yaml_abs_path(inputs_yaml,work_dir,inputs_yaml)
    copy_ignore_same_file(inputs_yaml,inputs_yaml_work_dir)
    #convert_examples_to_use_abs_path(inputs_yaml)

    if not input_yaml_path:
        os.rename(inputs_yaml_work_dir,input_yaml_path)

    if project_path:
        input_files_blob = targzip_project_files(project_id, project_path)

    requirements_list = []
    for single_workflow in workflow_params_data['workflows']:
        requirements_list.extend(workflow_params_data['workflows'][single_workflow]['input_config'])

    workflow_params_data.update(workflow_params_data['requirements'])
    workflow_params_data['inputs_yaml'] = input_yaml_path

    submit(pipeline_name, pipeline_version,job_uuid, jobstore_uuid, restart, work_dir, inputs_yaml, pipeline_settings, input_files_blob, workflow_params_data, workflow_params_data, requirements_list,cleanup)

if __name__ == "__main__":

    main()
