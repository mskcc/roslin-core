#!/usr/bin/env python
from __future__ import print_function
import os, sys, glob, uuid, argparse, subprocess
import hashlib
import datetime
import json
import tarfile
import base64
import time
import re
from subprocess import PIPE, Popen
import inspect
import logging
import signal
import shutil
from functools import partial
import socket
import getpass

if 'ROSLIN_CORE_BIN_PATH' not in os.environ:
    print("Roslin core settings not loaded")
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
ROSLIN_CORE_CONFIG_PATH = os.environ['ROSLIN_CORE_CONFIG_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import load_pipeline_settings, copy_ignore_same_file, run_command, run_command_realtime, print_error, send_user_kill_signal

MB_SIZE = (float(1024) ** 2)
MAX_META_FILE_SIZE = 5 * MB_SIZE


logger = logging.getLogger("roslin_submit")

def cleanup(clean_up_tuple, signal_num, frame):
    signal_name = "Unknown"
    if signal_num == signal.SIGINT:
        signal_name = "SIGINT"
    if signal_num == signal.SIGTERM:
        signal_name = "SIGTERM"
    signal_message = "Received signal: "+ signal_name
    print(signal_message)
    send_user_kill_signal(*clean_up_tuple)

def submit(project_id, project_uuid, project_path, pipeline_name, pipeline_version, batch_system, cwl_batch_system, jobstore_uuid, restart, debug_mode, work_dir, workflow_name, inputs_yaml, pipeline_settings, input_files_blob, foreground_mode, requirements_dict):
    from track_utils import  construct_project_doc, submission_file_name, get_current_time, add_user_event, update_run_result_doc, update_project_doc, construct_run_results_doc, update_latest_project, find_unique_name_in_dir, termination_file_name, old_jobs_folder, update_run_results_restart, update_run_data_doc, construct_run_data_doc
    from ruamel.yaml import safe_load
    log_folder = os.path.join(work_dir,'log')
    roslin_bin_path = pipeline_settings['ROSLIN_PIPELINE_BIN_PATH']
    roslin_output_path = os.path.join(work_dir,'outputs')
    roslin_work_path = os.path.join(work_dir,'tmp')
    roslin_tmp_path = os.path.join(roslin_bin_path,'tmp')
    roslin_leader_tmp_path = os.path.join(roslin_work_path,'leader')
    submission_log_path = os.path.join(log_folder,submission_file_name)
    run_attempt = 0
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    else:
        if os.path.exists(submission_log_path):
            with open(submission_log_path,"r") as submission_file_obj:
                submission_contents = json.load(submission_file_obj)
            run_attempt = submission_contents['run_attempt'] + 2
        old_jobs_folder_path = os.path.join(work_dir,old_jobs_folder_path)
        if not os.path.exists(old_jobs_folder_path):
            os.mkdir(old_jobs_folder_path)
        log_folder_basename = find_unique_name_in_dir('log',old_jobs_folder_path)
        old_log_folder = os.path.join(old_jobs_folder_path,log_folder_basename)
        shutil.move(log_folder,old_log_folder)
        os.mkdir(log_folder)
    if not os.path.exists(roslin_output_path):
        os.mkdir(roslin_output_path)
    log_stdout = "leader-stdout.log"
    log_stderr = "leader-stderr.log"
    input_meta_data = None
    roslin_leader_command = ["roslin_leader.py",
    "--id",project_id,
    "--jobstore-id",jobstore_uuid,
    "--uuid",project_uuid,
    "--inputs",inputs_yaml,
    "--project-output",roslin_output_path,
    "--project-workdir",roslin_work_path,
    "--project-tmpDir",roslin_tmp_path,
    "--workflow",workflow_name,
    "--pipeline-name",pipeline_name,
    "--pipeline-version",pipeline_version,
    "--batch-system",batch_system,
    "--retry-count",str(1),
    "--run-attempt",str(run_attempt),
    "--disableChaining",
    "--log-folder",log_folder]
    if cwl_batch_system:
        roslin_leader_command.extend(["--cwl-batch-system",cwl_batch_system])
    if debug_mode:
        roslin_leader_command.append('--debug_mode')
    for single_requirement_key in requirements_dict:
        requirement_value, requirement_option = requirements_dict[single_requirement_key]
        roslin_leader_command.extend([requirement_option,requirement_value])
    if restart:
        roslin_leader_command.append('--restart')
        user_event_name = "restart"
    leader_job_store = jobstore_uuid
    leader_job_store_path = "file:" + os.path.join(roslin_leader_tmp_path,leader_job_store)
    roslin_leader_command.append(leader_job_store_path)
    log_path_stdout = None
    log_path_stderr = None
    if foreground_mode:
        log_path_stdout = os.path.join(log_folder,log_stdout)
        log_path_stderr = os.path.join(log_folder,log_stderr)
    cwltoil_log_path = os.path.join(log_folder,'cwltoil.log')
    user = getpass.getuser()
    hostname = socket.gethostname()
    current_time = get_current_time()
    with open(inputs_yaml) as input_yaml_file:
        input_yaml_data = safe_load(input_yaml_file)
    submission_dict = {"time":current_time,"user":user,"hostname":hostname,"batch_system":batch_system,"env":dict(os.environ),"command":roslin_leader_command,"restart":restart,"run_attempt":run_attempt,"input_yaml":input_yaml_data,"input_meta":input_meta_data}
    if not restart:
        project_doc = construct_project_doc(logger,pipeline_name, pipeline_version, project_id, project_path, project_uuid, jobstore_uuid, work_dir, workflow_name, input_files_blob, restart)
        run_results_doc = construct_run_results_doc(pipeline_name, pipeline_version, project_id, project_path, project_uuid, jobstore_uuid, work_dir, workflow_name, input_files_blob, user, current_time, cwltoil_log_path, log_path_stdout, log_path_stderr)
        run_data_doc = construct_run_data_doc(project_uuid, jobstore_uuid, pipeline_version, project_id)
        update_project_doc(logger,project_uuid,project_doc)
        update_run_result_doc(logger,project_uuid,run_results_doc)
        update_run_data_doc(logger,project_uuid,run_data_doc)
        update_latest_project(logger,project_uuid,project_id)
        user_event_name = "submit"
    else:
        update_run_results_restart(logger,project_uuid,current_time)
        user_event_name = "restart"
    add_user_event(logger,project_uuid,submission_dict,user_event_name)
    running_command_template =  "Running workflow ( {} ) of Roslin {} [ version {} ] on {}\nProject: {} [{}]\nOutput: {}\nLogs: {}"
    running_command_str = running_command_template.format(workflow_name,pipeline_name,pipeline_version,batch_system,project_id,project_uuid,roslin_output_path,log_folder)
    with open(submission_log_path,"w") as submission_file:
        json.dump(submission_dict,submission_file)
    if foreground_mode:
        clean_up_tuple = (project_id, project_uuid, pipeline_name, pipeline_version, True)
        signal.signal(signal.SIGINT, partial(cleanup, clean_up_tuple))
        signal.signal(signal.SIGTERM, partial(cleanup, clean_up_tuple))
        print(running_command_str)
        command_output = run_command_realtime(roslin_leader_command, False)
        exit_code = command_output['errorcode']
        exit(exit_code)
    else:
        leader_process = run_command(roslin_leader_command,None,None,False,False)
        print(running_command_str)

def generate_sha1(filename):

    # 64kb chunks
    buf_size = 65536

    sha1 = hashlib.sha1()

    with open(filename, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()

def targzip_project_files(project_id, project_path):

    files = glob.glob(os.path.join(project_path, "*"))
    input_files = {}
    input_file_objs = []
    tgz_path = "{}.tgz".format(os.path.join(project_path, project_id))
    tar = tarfile.open(tgz_path, mode="w:gz", dereference=True)
    for filename in files:
        single_file_path = os.path.abspath(filename)
        if os.path.isfile(single_file_path):
            single_file_name = os.path.dirname(filename)
            single_file_name_no_ext = os.path.splitext(single_file_name)[0]
            single_file_size = os.path.getsize(filename)
            single_file_sha1 = "sha1$" + generate_sha1(filename)
            if single_file_size < MAX_META_FILE_SIZE:
                tar.add(filename)
                file_obj = {"path": single_file_path,"checksum": single_file_sha1, "name":filename}
            input_file_objs.append(file_obj)
    tar.close()

    tgz_blob = None

    with open(tgz_path, "rb") as tgz_file:
        tgz_blob = base64.b64encode(tgz_file.read())

    input_files['blob'] = tgz_blob
    input_files['files'] = input_file_objs
    return input_files


'''def convert_examples_to_use_abs_path(inputs_yaml_path):
    "convert example inputs.yaml to use absolute path"

    current_directory = os.getcwd()
    inputs_yaml_directory = os.path.dirname(inputs_yaml_path)
    os.chdir(inputs_yaml_directory)

    output = []

    # fixme: best way is to look for:
    #   class: File
    #   path: ../abc/def/123.fastq
    with open(inputs_yaml_path, "r") as yaml_file:
        lines = yaml_file.readlines()
        prev_line = ""
        for line in lines:
            line = line.rstrip("\n")

            # if "class: File" in prev_line:
            #     # fixme: pre-compile
            #     # path: ../ or path: ./
            #     pattern = r"path: (\.\.?/.*)"
            #     match = re.search(pattern, line)
            #     if match:
            #         path = os.path.abspath(match.group(1))
            #         line = re.sub(pattern, "path: {}".format(path), line)

            # fixme: pre-compile
            # path: ../ or path: ./
            pattern = r"path: (\.\.?/.*)"
            match = re.search(pattern, line)
            if match:
                path = os.path.abspath(match.group(1))
                line = re.sub(pattern, "path: {}".format(path), line)

            output.append(line)
            prev_line = line

    with open(inputs_yaml_path, "w") as yaml_file:
        yaml_file.write("\n".join(output))

    os.chdir(current_directory)'''

def get_pipeline_name_and_versions():
    pipelines = {}
    for single_pipeline_dir in os.listdir(ROSLIN_CORE_CONFIG_PATH):
        dir_path = os.path.join(ROSLIN_CORE_CONFIG_PATH,single_pipeline_dir)
        if os.path.isdir(dir_path):
            pipelines[single_pipeline_dir] = []
            for single_version_dir in os.listdir(dir_path):
                single_version_path = os.path.join(dir_path,single_version_dir)
                if os.path.isdir(single_version_path):
                    pipelines[single_pipeline_dir].append(single_version_dir)
    pipeline_name_choices = []
    pipeline_version_choices =[]
    pipeline_version_help = "Pipeline versions:\n"
    pipeline_name_help = "Pipeline name\n"
    pipeline_name_choices = pipelines.keys()
    for single_pipeline_name_key in pipelines:
        single_pipeline_name = pipelines[single_pipeline_name_key]
        single_pipeline_help_str = single_pipeline_name_key + " - " + " ,".join(single_pipeline_name) + "\n"
        for single_pipeline_version in single_pipeline_name:
            if single_pipeline_version not in pipeline_version_choices:
                pipeline_version_choices.append(single_pipeline_version)

    return {'pipeline_name_choices':pipeline_name_choices,'pipeline_version_choices':pipeline_version_choices,'pipeline_name_help':pipeline_name_help,'pipeline_version_help':pipeline_version_help}


def main():
    "main function"

    preparser = argparse.ArgumentParser(description='roslin submit', add_help=False)

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
    import roslin_workflows
    from toil.batchSystems import registry
    from track_utils import  RoslinWorkflow, construct_project_doc, submission_file_name, get_current_time, add_user_event, update_run_result_doc, update_project_doc, construct_run_results_doc, update_latest_project
    from core_utils import convert_yaml_abs_path
    roslin_workflow_list_unfiltered = inspect.getmembers(roslin_workflows, inspect.isclass)
    roslin_workflow_list = []
    for single_class in roslin_workflow_list_unfiltered:
        class_name = single_class[0]
        class_obj = single_class[1]
        if class_obj == RoslinWorkflow:
            continue
        if issubclass(class_obj,RoslinWorkflow):
            roslin_workflow_list.append(class_name)

    parser = argparse.ArgumentParser(parents=[ preparser ], add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--id",
        action="store",
        dest="project_id",
        help="Project ID (e.g. Proj_5088_B)",
        required=True
    )

    parser.add_argument(
        "--inputs",
        action="store",
        dest="inputs_yaml",
        help="The path to your input yaml file (e.g. /ifs/projects/CMO/Proj_5088_B/inputs.yaml)",
        required=True
    )

    parser.add_argument(
        "--workflow",
        action="store",
        dest="workflow_name",
        choices=list(roslin_workflow_list),
        help="Workflow name (e.g. project-workflow)",
        required=True
    )

    parser.add_argument(
        "--restart",
        action="store",
        dest="restart_job_uuid",
        help="project uuid for restart",
        required=False
    )

    parser.add_argument(
        "--batch-system",
        action="store",
        dest="batch_system",
        choices=list(registry._UNIQUE_NAME),
        help="The batch system to submit the job",
        required=True
    )

    parser.add_argument(
        "--cwl-batch-system",
        action="store",
        dest="cwl_batch_system",
        choices=list(registry._UNIQUE_NAME),
        help="The batch system to submit the cwl jobs (uses --batch-system if not set)",
        required=False
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        dest="debug_mode",
        help="Run the runner in debug mode"
    )

    parser.add_argument(
        "--foreground-mode",
        action="store_true",
        dest="foreground_mode",
        help="Runs the pipeline the the foreground",
        required=False
    )

    parser.add_argument(
        "--path",
        action="store",
        dest="project_path",
        help="Path to Project files (to store in database)",
        required=False
    )

    params, _ = parser.parse_known_args()
    roslin_workflow_class = getattr(roslin_workflows,params.workflow_name)
    roslin_workflow = roslin_workflow_class(None)
    workflow_parser = argparse.ArgumentParser(parents=[ parser ], add_help=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    requirements_obj = roslin_workflow.add_requirement(workflow_parser)
    requirements_dict = {}
    if requirements_obj:
        workflow_parser, requirements_list = requirements_obj
        workflow_params = workflow_parser.parse_args()
        for parser_action, parser_type, parser_dest, parser_option, parser_help, parser_required, is_path in requirements_list:
            workflow_param_key = parser_dest
            workflow_param_value = workflow_params.__dict__[workflow_param_key]
            if is_path:
                if not os.path.exists(workflow_param_value):
                    print_error("ERROR: Could not fine "+ str(workflow_param_value))
                    sys.exit(1)
                workflow_param_value = os.path.abspath(workflow_param_value)
            requirements_dict[workflow_param_key] = (workflow_param_value, parser_option)
    project_id = params.project_id
    inputs_yaml = os.path.abspath(params.inputs_yaml)
    restart_job_uuid = params.restart_job_uuid
    inputs_yaml_dirname = os.path.dirname(inputs_yaml)
    inputs_yaml_basename = os.path.basename(inputs_yaml)
    if not os.path.exists(inputs_yaml):
        print_error("ERROR: Could not find "+inputs_yaml)
        sys.exit(1)

    work_base_dir = os.path.abspath(pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"])
    # create a new unique job uuid
    if not restart_job_uuid:
        jobstore_uuid = str(uuid.uuid1())
        project_uuid = str(uuid.uuid1())
        restart = False
    else:
        previous_work_base_dir = os.path.join(work_base_dir, restart_job_uuid[:8], restart_job_uuid)
        error_extra_info =  "for " + pipeline_name + " (" + pipeline_version + ") "
        if not os.path.exists(previous_work_base_dir):
            print_error("ERROR: Could not find project with uuid: " + restart_job_uuid + error_extra_info)
            exit(1)
        previous_jobstore_uuid_path = os.path.join(previous_work_base_dir,"jobstore_uuid")
        if not os.path.exists(previous_jobstore_uuid_path):
            print_error("ERROR: Could not find jobstore_uuid file in "+previous_work_base_dir +"\n"+error_extra_info)
            exit(1)
        with open(previous_jobstore_uuid_path) as previous_jobstore_uuid_file:
            jobstore_uuid = previous_jobstore_uuid_file.readline().strip()
        project_uuid = restart_job_uuid
        restart = True

    work_dir = os.path.join(work_base_dir, project_uuid[:8], project_uuid)
    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    if params.project_path:
        project_path = os.path.abspath(params.project_path)
        input_metadata_filenames = [
            "{}_request.txt".format(project_id),
            "{}_sample_grouping.txt".format(project_id),
            "{}_sample_mapping.txt".format(project_id),
            "{}_sample_pairing.txt".format(project_id),
            "{}_sample_data_clinical.txt".format(project_id)
        ]
        for filename in input_metadata_filenames:
            input_metadata_location = os.path.join(project_path,filename)
            input_metadata_work_dir_location = os.path.join(work_dir,filename)
            if os.path.exists(input_metadata_location):
                copy_ignore_same_file(input_metadata_location,input_metadata_work_dir_location)
    else:
        project_path = None

    jobstore_uuid_path = os.path.join(work_dir,'jobstore_uuid')
    with open(jobstore_uuid_path,"w") as jobstore_uuid_file:
        jobstore_uuid_file.write(jobstore_uuid)

    #if restart and previous_work_base_dir:
    #    previous_outputs = os.path.join(previous_work_base_dir,'outputs')
    #    current_output = os.path.join(work_dir,'outputs')
    #    shutil.copytree(previous_outputs,current_output)

    # copy input metadata files (mapping, grouping, paring, request, and inputs.yaml)

    input_yaml_work_dir_location = os.path.join(work_dir,inputs_yaml_basename)
    #copy_ignore_same_file(inputs_yaml,input_yaml_work_dir_location)
    # convert any relative path in inputs.yaml (e.g. path: ../abc)
    # to absolute path (e.g. path: /ifs/abc)
    convert_yaml_abs_path(inputs_yaml,work_dir,inputs_yaml)
    copy_ignore_same_file(inputs_yaml,input_yaml_work_dir_location)
    #convert_examples_to_use_abs_path(inputs_yaml)

    if project_path:
        input_files_blob = targzip_project_files(project_id, project_path)
    # submit
    submit(project_id, project_uuid, params.project_path, pipeline_name, pipeline_version, params.batch_system, params.cwl_batch_system, jobstore_uuid, restart, params.debug_mode, work_dir, params.workflow_name, inputs_yaml, pipeline_settings, input_files_blob, params.foreground_mode,requirements_dict)

if __name__ == "__main__":

    main()
