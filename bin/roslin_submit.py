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
import logging
import signal
import shutil
from functools import partial
import socket
import getpass
import glob

if 'ROSLIN_CORE_BIN_PATH' not in os.environ:
    print("Roslin core settings not loaded")
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
ROSLIN_CORE_CONFIG_PATH = os.environ['ROSLIN_CORE_CONFIG_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import load_pipeline_settings, copy_ignore_same_file, run_command, run_command_realtime, print_error, send_user_kill_signal, check_if_argument_file_exists, check_yaml_boolean_value, load_yaml, save_yaml, check_tmp_env, get_common_args, parse_workflow_args, get_submission_args, add_specific_args, get_leader_args, get_args_dict

GB_SIZE_MB = 1024
MB_SIZE_B = (float(GB_SIZE_MB) ** 2)
MAX_META_FILE_SIZE = 5 * MB_SIZE_B
JOBSTORE_UUID_FILE_NAME = 'jobstore_uuid'
INPUT_YAML_FILE_NAME = 'inputs.yaml'
WORKFLOW_PARAMS_FILE_NAME = 'workflow_params.json'

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


def handle_change_max_memory(work_dir,max_memory_str,max_cpu_str,input_yaml_path,debug_mode):
    max_memory = int(max_memory_str[:-1]) * GB_SIZE_MB
    max_cpu = int(max_cpu_str)
    cwl_directory = os.environ['ROSLIN_PIPELINE_CWL_PATH']
    new_cwl_directory = os.path.join(work_dir,'cwl')
    print("Creating a cache for cwl to modify: " + new_cwl_directory)
    shutil.copytree(cwl_directory,new_cwl_directory)
    print("Setting cwl to max memory " + str(max_memory) + " and max cpu " + str(max_cpu))
    for root, dirnames, filenames in os.walk(new_cwl_directory):
        for single_file in filenames:
            if '.cwl' in single_file:
                cwl_file_path = os.path.join(root,single_file)
                yaml_contents = load_yaml(cwl_file_path)
                changed = False
                if 'requirements' in yaml_contents:
                    resource_requirement = None
                    requirements_obj = yaml_contents['requirements']
                    if type(requirements_obj) is list:
                        for single_item in requirements_obj:
                            if 'class' in single_item:
                                if single_item['class'] == 'ResourceRequirement':
                                    resource_requirement = single_item
                    if type(requirements_obj) is dict:
                        if 'ResourceRequirement' in yaml_contents['requirements']:
                            resource_requirement = yaml_contents['requirements']['ResourceRequirement']
                    if resource_requirement:
                        if 'ramMin' in resource_requirement:
                            if type(resource_requirement['ramMin']) is int:
                                if int(resource_requirement['ramMin']) > max_memory:
                                    resource_requirement['ramMin'] = max_memory
                                    changed = True
                        if 'coresMin' in resource_requirement:
                            if type(resource_requirement['coresMin']) is int:
                                if int(resource_requirement['coresMin']) > max_cpu:
                                    resource_requirement['coresMin'] = max_cpu
                                    changed = True
                if changed:
                    if debug_mode:
                        print('Modiying: '+ cwl_file_path)
                    save_yaml(cwl_file_path,yaml_contents)
    input_yaml_contents = load_yaml(input_yaml_path)
    abra_ram_min = input_yaml_contents['runparams']['abra_ram_min']
    if abra_ram_min > max_memory:
        print("Modiying input yaml as " + str(abra_ram_min) + " is greater than max memory of " + str(max_memory) )
        input_yaml_contents['runparams']['abra_ram_min'] = max_memory
        save_yaml(input_yaml_path,input_yaml_contents)
    os.environ['ROSLIN_PIPELINE_CWL_PATH'] = new_cwl_directory


def submit(pipeline_name, pipeline_version,job_uuid, jobstore_uuid, restart, work_dir, inputs_yaml, pipeline_settings, input_files_blob, requirements_dict,submission_requirements_dict,workflow_args):
    from track_utils import  construct_project_doc, submission_file_name, get_current_time, add_user_event, update_run_result_doc, update_project_doc, construct_run_results_doc, update_latest_project, find_unique_name_in_dir, termination_file_name, old_jobs_folder, update_run_results_restart, update_run_profile_doc, construct_run_profile_doc
    leader_args = get_leader_args()
    common_args = get_common_args()
    leader_args_dict = get_args_dict(leader_args)
    common_args_dict = get_args_dict(common_args)
    workflow_args_dict = {}
    if workflow_args:
        workflow_args_dict = get_args_dict(workflow_args)
    combined_args_dict = common_args_dict
    combined_args_dict.update(workflow_args_dict)
    log_folder = os.path.join(work_dir,'log')
    roslin_bin_path = pipeline_settings['ROSLIN_PIPELINE_BIN_PATH']
    roslin_output_path = os.path.join(work_dir,'outputs')
    roslin_work_path = os.path.join(work_dir,'tmp')
    roslin_tmp_path = os.path.join(roslin_bin_path,'tmp')
    roslin_leader_tmp_path = os.path.join(roslin_work_path,'leader')
    submission_log_path = os.path.join(log_folder,submission_file_name)
    workflow_results_path = None
    project_id = requirements_dict['project_id']
    project_path = requirements_dict['project_path']
    debug_mode = requirements_dict['debug_mode']
    batch_system = requirements_dict['batch_system']
    workflow_name = requirements_dict['workflow_name']
    test_mode = requirements_dict['test_mode']
    results_dir = submission_requirements_dict['results_dir']
    max_cpu = submission_requirements_dict['max_cpu']
    max_mem = submission_requirements_dict['max_mem']
    foreground_mode = submission_requirements_dict['foreground_mode']
    if results_dir:
        workflow_results_folder = os.path.join(project_id,job_uuid)
        workflow_results_path = os.path.join(results_dir,workflow_results_folder)
        if not os.access(results_dir, os.W_OK):
            print_error("ERROR: Can not write to " + str(results_dir))
            exit(1)
    run_attempt = 0
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    else:
        if os.path.exists(submission_log_path):
            with open(submission_log_path,"r") as submission_file_obj:
                submission_contents = json.load(submission_file_obj)
            run_attempt = submission_contents['run_attempt'] + 2
        old_jobs_folder_path = os.path.join(work_dir,old_jobs_folder)
        if not os.path.exists(old_jobs_folder_path):
            os.mkdir(old_jobs_folder_path)
        log_folder_basename = find_unique_name_in_dir('log',old_jobs_folder_path)
        old_log_folder = os.path.join(old_jobs_folder_path,log_folder_basename)
        shutil.move(log_folder,old_log_folder)
        os.mkdir(log_folder)
    if not os.path.exists(roslin_output_path):
        os.mkdir(roslin_output_path)
    if not os.path.exists(roslin_work_path):
        os.mkdir(roslin_work_path)
    if not os.path.exists(roslin_tmp_path):
        os.mkdir(roslin_tmp_path)
    log_stdout = "leader-stdout.log"
    log_stderr = "leader-stderr.log"
    input_meta_data = None
    roslin_leader_command = ["roslin_leader.py",
    leader_args_dict['pipeline_name'],pipeline_name,
    leader_args_dict['pipeline_version'],pipeline_version,
    leader_args_dict['jobstore_uuid'],jobstore_uuid,
    leader_args_dict['project_uuid'],job_uuid,
    leader_args_dict['project_output'],roslin_output_path,
    leader_args_dict['project_workdir'],roslin_work_path,
    leader_args_dict['project_tmpdir'],roslin_tmp_path,
    leader_args_dict['retry_count'],str(1),
    leader_args_dict['run_attempt'],str(run_attempt),
    "--disableChaining",
    leader_args_dict['log_folder'],log_folder]
    if results_dir:
        roslin_leader_command.extend([leader_args_dict['project_results'],workflow_results_path])
    if max_cpu:
        roslin_leader_command.extend(['--maxCores',max_cpu])
    if max_mem:
        roslin_leader_command.extend(['--maxMemory',max_mem])
    if test_mode:
        if 'ROSLIN_TEST_USE_DOCKER' in os.environ:
            if check_yaml_boolean_value(os.environ['ROSLIN_TEST_USE_DOCKER']):
                roslin_leader_command.append('--use-docker')
        if 'ROSLIN_TEST_DOCKER_REGISTRY' in os.environ:
            if os.environ['ROSLIN_TEST_DOCKER_REGISTRY'] != 'None' and os.environ['ROSLIN_TEST_DOCKER_REGISTRY'].strip() != '':
                docker_registry = os.environ['ROSLIN_TEST_DOCKER_REGISTRY']
                roslin_leader_command.extend(['--docker-registry',docker_registry])
    for single_requirement_key in combined_args_dict:
        if single_requirement_key in requirements_dict:
            requirement_value = requirements_dict[single_requirement_key]
            requirement_option = combined_args_dict[single_requirement_key]
            if requirement_value != None:
                if isinstance(requirement_value, list):
                    for single_requirement_value in requirement_value:
                        roslin_leader_command.extend([requirement_option,single_requirement_value])
                elif isinstance(requirement_value,bool):
                    if requirement_value == True:
                        roslin_leader_command.extend([requirement_option])
                else:
                    roslin_leader_command.extend([requirement_option,requirement_value])
    if restart:
        roslin_leader_command.append('--restart')
        user_event_name = "restart"
    leader_job_store = jobstore_uuid
    leader_job_store_path = "file:" +os.path.join(roslin_leader_tmp_path,leader_job_store)
    roslin_leader_command.append(leader_job_store_path)
    log_path_stdout = os.path.join(log_folder,log_stdout)
    log_path_stderr = os.path.join(log_folder,log_stderr)
    cwltoil_log_path = os.path.join(log_folder,'cwltoil.log')
    user = getpass.getuser()
    hostname = socket.gethostname()
    current_time = get_current_time()
    input_yaml_data = load_yaml(inputs_yaml)
    submission_dict = {"time":current_time,"user":user,"hostname":hostname,"batch_system":batch_system,"env":dict(os.environ),"command":roslin_leader_command,"restart":restart,"run_attempt":run_attempt,"input_yaml":input_yaml_data,"input_meta":input_meta_data,"log_dir":log_folder,"work_dir":work_dir,"project_output_dir":roslin_output_path,"project_results_dir":workflow_results_path,"project_id":project_id,"job_uuid":job_uuid,"pipeline_name":pipeline_name,"pipeline_version":pipeline_version,"workflow":workflow_name}
    with open("submission.json","w") as submission_file:
        json.dump(submission_dict,submission_file)
    if not restart:
        project_doc = construct_project_doc(logger,pipeline_name, pipeline_version, project_id, project_path, job_uuid, jobstore_uuid, work_dir, workflow_name, input_files_blob, restart, workflow_results_path)
        run_results_doc = construct_run_results_doc(pipeline_name, pipeline_version, project_id, project_path, job_uuid, jobstore_uuid, work_dir, workflow_name, input_files_blob, user, current_time, cwltoil_log_path, log_path_stdout, log_path_stderr)
        run_profile_doc = construct_run_profile_doc(logger,job_uuid,pipeline_settings)
        update_project_doc(logger,job_uuid,project_doc)
        update_run_result_doc(logger,job_uuid,run_results_doc)
        update_latest_project(logger,job_uuid,project_id)
        update_run_profile_doc(logger,job_uuid,run_profile_doc)
        user_event_name = "submit"
    else:
        update_run_results_restart(logger,job_uuid,current_time)
        user_event_name = "restart"
    add_user_event(logger,job_uuid,submission_dict,user_event_name)
    running_command_template =  "Running workflow ( {} ) of Roslin {} [ version {} ] on {}\nProject: {} [{}]\nOutput: {}\nLogs: {}"
    running_command_str = running_command_template.format(workflow_name,pipeline_name,pipeline_version,batch_system,project_id,job_uuid,roslin_output_path,log_folder)
    if workflow_results_path:
        running_command_str = running_command_str + "\nResults: {}".format(workflow_results_path)
    with open(submission_log_path,"w") as submission_file:
        json.dump(submission_dict,submission_file)
    if debug_mode:
        print("Leader command:")
        print(roslin_leader_command)
    if foreground_mode:
        clean_up_tuple = (project_id, job_uuid, pipeline_name, pipeline_version, True)
        signal.signal(signal.SIGINT, partial(cleanup, clean_up_tuple))
        signal.signal(signal.SIGTERM, partial(cleanup, clean_up_tuple))
        if debug_mode:
            print("Project info:")
        print(running_command_str)
        command_output = run_command_realtime(roslin_leader_command, False)
        exit_code = command_output['errorcode']
        output = command_output['output']
        error = command_output['error']
        if error != None:
            print_error(error)
        with open(log_path_stdout,"w") as log_stdout:
            log_stdout.write(output)
        with open(log_path_stderr,"w") as log_stderr:
            log_stderr.write(error)
        exit(exit_code)
    else:
        leader_process = run_command(roslin_leader_command,log_path_stdout,log_path_stderr,False,False)
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
            single_file_name = os.path.basename(filename)
            single_file_size = os.path.getsize(filename)
            single_file_sha1 = "sha1$" + generate_sha1(filename)
            if single_file_size < MAX_META_FILE_SIZE:
                tar.add(single_file_path, arcname=single_file_name)
                file_obj = {"path": single_file_path,"checksum": single_file_sha1, "name":single_file_name}
                input_file_objs.append(file_obj)
    tar.close()

    tgz_blob = None

    with open(tgz_path, "rb") as tgz_file:
        tgz_blob = base64.b64encode(tgz_file.read())

    input_files['blob'] = tgz_blob
    input_files['files'] = input_file_objs
    return input_files

def get_input_metadata(project_path):
    input_metadata_filenames = []
    input_metadata_file_paths = []
    input_metadata_file_paths.append(os.path.join(project_path,"*_request.txt"))
    input_metadata_file_paths.append(os.path.join(project_path,"*_grouping.txt"))
    input_metadata_file_paths.append(os.path.join(project_path,"*_mapping.txt"))
    input_metadata_file_paths.append(os.path.join(project_path,"*_pairing.txt"))
    input_metadata_file_paths.append(os.path.join(project_path,"*_clinical.txt"))
    for single_file_path in input_metadata_file_paths:
        meta_data_file_list = glob.glob(single_file_path)
        for single_file in meta_data_file_list:
            input_metadata_filenames.append(os.path.basename(single_file))
    return input_metadata_filenames

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
    from track_utils import  construct_project_doc, submission_file_name, get_current_time, add_user_event, update_run_result_doc, update_project_doc, construct_run_results_doc, update_latest_project
    from core_utils import convert_yaml_abs_path
    import roslin_workflows
    parser = argparse.ArgumentParser(parents=[ preparser ], add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    common_requirements_list = get_common_args()
    submission_requirements_list = get_submission_args()
    parser = add_specific_args(parser,common_requirements_list)
    parser = add_specific_args(parser,submission_requirements_list)
    params, _ = parser.parse_known_args()
    requirements_dict = parse_workflow_args(params, common_requirements_list)
    submission_requirements_dict = parse_workflow_args(params, submission_requirements_list)
    project_id = requirements_dict['project_id']
    work_base_dir = os.path.abspath(pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"])
    inputs_yaml = None
    # create a new unique job uuid
    jobstore_uuid = str(uuid.uuid1())
    job_uuid = str(uuid.uuid1())
    restart = False
    inputs_yaml = os.path.abspath(params.inputs_yaml)
    inputs_yaml_dirname = os.path.dirname(inputs_yaml)
    inputs_yaml_basename = os.path.basename(inputs_yaml)
    check_tmp_env(None)
    roslin_workflow_class = getattr(roslin_workflows,requirements_dict['workflow_name'])
    roslin_workflow = roslin_workflow_class(None)
    workflow_parser = argparse.ArgumentParser(parents=[ parser ], add_help=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    requirements_obj = roslin_workflow.add_requirement(workflow_parser)
    requirements_list = None
    if requirements_obj:
        workflow_parser, requirements_list = requirements_obj
        workflow_params = workflow_parser.parse_args()
        requirements_dict.update(parse_workflow_args(workflow_params,requirements_list))

    if requirements_dict['force_overwrite_results'] and not requirements_dict['results_dir']:
        print_error("ERROR: You need to specify an output directory to force overwrite")
        sys.exit(1)

    work_dir = os.path.join(work_base_dir, job_uuid[:8], job_uuid)
    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    if requirements_dict['project_path']:
        project_path = os.path.abspath(requirements_dict['project_path'])
        input_metadata_filenames = get_input_metadata(project_path)
        for filename in input_metadata_filenames:
            input_metadata_location = os.path.join(project_path,filename)
            input_metadata_work_dir_location = os.path.join(work_dir,filename)
            if os.path.exists(input_metadata_location):
                copy_ignore_same_file(input_metadata_location,input_metadata_work_dir_location)
    else:
        project_path = None
        input_files_blob = None

    jobstore_uuid_path = os.path.join(work_dir,JOBSTORE_UUID_FILE_NAME)
    input_yaml_path = os.path.join(work_dir,INPUT_YAML_FILE_NAME)
    with open(jobstore_uuid_path,"w") as jobstore_uuid_file:
        jobstore_uuid_file.write(jobstore_uuid)

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

    if submission_requirements_dict['max_mem'] != parser.get_default('max_mem') or submission_requirements_dict['max_cpu'] != parser.get_default('max_cpu'):
        handle_change_max_memory(work_dir,submission_requirements_dict['max_mem'],submission_requirements_dict['max_cpu'],inputs_yaml_work_dir,requirements_dict['debug_mode'])

    # submit
    submit(pipeline_name, pipeline_version, job_uuid, jobstore_uuid, False, work_dir, inputs_yaml_work_dir, pipeline_settings, input_files_blob, requirements_dict, submission_requirements_dict,requirements_list)

if __name__ == "__main__":

    main()
