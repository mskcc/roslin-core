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
from core_utils import load_pipeline_settings, copy_ignore_same_file, run_command, run_command_realtime, print_error, send_user_kill_signal, check_if_argument_file_exists, check_yaml_boolean_value, load_yaml, save_yaml

GB_SIZE_MB = 1024
MB_SIZE_B = (float(1024) ** 2)
MAX_META_FILE_SIZE = 5 * MB_SIZE_B


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
                    if 'ResourceRequirement' in yaml_contents['requirements']:
                        resource_requirement = yaml_contents['requirements']['ResourceRequirement']
                        if 'ramMin' in resource_requirement:
                            if type(resource_requirement['ramMin']) is int:
                                if int(resource_requirement['ramMin']) > max_memory:
                                    yaml_contents['requirements']['ResourceRequirement']['ramMin'] = max_memory
                                    changed = True
                        if 'coresMin' in resource_requirement:
                            if type(resource_requirement['coresMin']) is int:
                                if int(resource_requirement['coresMin']) > max_cpu:
                                    yaml_contents['requirements']['ResourceRequirement']['coresMin'] = max_cpu
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



def submit(project_id, job_uuid, project_path, pipeline_name, pipeline_version, batch_system, cwl_batch_system, jobstore_uuid, restart, debug_mode, work_dir, workflow_name, inputs_yaml, pipeline_settings, input_files_blob, foreground_mode, requirements_dict, test_mode, results_dir, force_overwrite_results, on_start, on_complete, on_fail, on_success, use_docker, docker_registry, max_mem, max_cpu):
    from track_utils import  construct_project_doc, submission_file_name, get_current_time, add_user_event, update_run_result_doc, update_project_doc, construct_run_results_doc, update_latest_project, find_unique_name_in_dir, termination_file_name, old_jobs_folder, update_run_results_restart, update_run_data_doc, update_run_profile_doc, construct_run_data_doc, construct_run_profile_doc
    log_folder = os.path.join(work_dir,'log')
    roslin_bin_path = pipeline_settings['ROSLIN_PIPELINE_BIN_PATH']
    roslin_output_path = os.path.join(work_dir,'outputs')
    roslin_work_path = os.path.join(work_dir,'tmp')
    roslin_tmp_path = os.path.join(roslin_bin_path,'tmp')
    roslin_leader_tmp_path = os.path.join(roslin_work_path,'leader')
    submission_log_path = os.path.join(log_folder,submission_file_name)
    workflow_results_path = None
    if results_dir:
        workflow_results_folder = project_id+"."+job_uuid
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
    log_stdout = "leader-stdout.log"
    log_stderr = "leader-stderr.log"
    input_meta_data = None
    roslin_leader_command = ["roslin_leader.py",
    "--id",project_id,
    "--jobstore-id",jobstore_uuid,
    "--uuid",job_uuid,
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
    if results_dir:
        roslin_leader_command.extend(["--project-results",workflow_results_path])
    if force_overwrite_results:
        roslin_leader_command.append('--force-overwrite-results')
    if debug_mode:
        roslin_leader_command.append('--debug-mode')
    if use_docker:
        roslin_leader_command.append('--use-docker')
    if docker_registry:
        roslin_leader_command.extend(['--docker-registry',docker_registry])
    if max_cpu:
        roslin_leader_command.extend(['--maxCores',max_cpu])
    if max_mem:
        roslin_leader_command.extend(['--maxMemory',max_mem])
    if test_mode:
        roslin_leader_command.append('--test-mode')
        if check_yaml_boolean_value(os.environ['ROSLIN_TEST_USE_DOCKER']):
            roslin_leader_command.append('--use-docker')
        if os.environ['ROSLIN_TEST_DOCKER_REGISTRY'] != 'None' and os.environ['ROSLIN_TEST_DOCKER_REGISTRY'].strip() != '':
            docker_registry = os.environ['ROSLIN_TEST_DOCKER_REGISTRY']
            roslin_leader_command.extend(['--docker-registry',docker_registry])
    for single_requirement_key in requirements_dict:
        requirement_value, requirement_option = requirements_dict[single_requirement_key]
        roslin_leader_command.extend([requirement_option,requirement_value])
    if restart:
        roslin_leader_command.append('--restart')
        user_event_name = "restart"
    if on_start:
        on_start_abspath = os.path.abspath(on_start)
        roslin_leader_command.extend(['--on_start', on_start_abspath])
    if on_complete:
        on_complete_abspath = os.path.abspath(on_complete)
        roslin_leader_command.extend(['--on-complete', on_complete_abspath])
    if on_fail:
        on_fail_abspath = os.path.abspath(on_fail)
        roslin_leader_command.extend(['--on-fail', on_fail_abspath])
    if on_success:
        on_success_abspath = os.path.abspath(on_success)
        roslin_leader_command.extend(['--on-success', on_success_abspath])
    leader_job_store = jobstore_uuid
    leader_job_store_path = "file:" + os.path.join(roslin_leader_tmp_path,leader_job_store)
    roslin_leader_command.append(leader_job_store_path)
    log_path_stdout = None
    log_path_stderr = None
    if not foreground_mode:
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
        run_data_doc = construct_run_data_doc(job_uuid, jobstore_uuid, pipeline_version, project_id)
        run_profile_doc = construct_run_profile_doc(logger,job_uuid,pipeline_settings)
        update_project_doc(logger,job_uuid,project_doc)
        update_run_result_doc(logger,job_uuid,run_results_doc)
        update_run_data_doc(logger,job_uuid,run_data_doc)
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
    if foreground_mode:
        clean_up_tuple = (project_id, job_uuid, pipeline_name, pipeline_version, True)
        signal.signal(signal.SIGINT, partial(cleanup, clean_up_tuple))
        signal.signal(signal.SIGTERM, partial(cleanup, clean_up_tuple))
        print(running_command_str)
        command_output = run_command_realtime(roslin_leader_command, False)
        exit_code = command_output['errorcode']
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

def mem_format_type(value):
    if not re.match(r"\d+[gG]",value):
        print_error(str(value) + " is not in a valid format for --max-mem (e.g. 8G)")
        raise argparse.ArgumentTypeError
    return value


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
        "--test-mode",
        action="store_true",
        dest="test_mode",
        help="Run the runner in test mode"
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
    parser.add_argument(
        "--results",
        action="store",
        dest="results_dir",
        help="Path to the directory to store results",
        required=False
    )
    parser.add_argument(
        "--force-overwrite-results",
        action="store_true",
        dest="force_overwrite_results",
        help="Force overwrite if results folder already exists",
        required=False
    )
    parser.add_argument(
        "--on-start",
        action="store",
        dest="on_start",
        help="Python script to run when the workflow starts",
        required=False
    )
    parser.add_argument(
        "--on-complete",
        action="store",
        dest="on_complete",
        help="Python script to run when the workflow completes (either fail or succeed)",
        required=False
    )
    parser.add_argument(
        "--on-fail",
        action="store",
        dest="on_fail",
        help="Python script to run when the workflow fails",
        required=False
    )
    parser.add_argument(
        "--on-success",
        action="store",
        dest="on_success",
        help="Python script to run when the workflow succeeds",
        required=False
    )
    parser.add_argument(
        "--use-docker",
        action="store_true",
        dest="use_docker",
        help="Use Docker instead of singularity",
        required=False
    )
    parser.add_argument(
        "--docker-registry",
        action="store",
        dest="docker_registry",
        help="Dockerhub registry to pull ( invoked only with --use-docker)",
        required=False
    )
    parser.add_argument(
        "--max-mem",
        action="store",
        dest="max_mem",
        default="256G",
        help="The maximum amount of memory to request in GB (e.g. 8G)",
        type=mem_format_type,
        required=False
    )
    parser.add_argument(
        "--max-cpu",
        action="store",
        dest="max_cpu",
        default="14",
        help="The maximum amount of cpu to request",
        required=False
    )
    params, _ = parser.parse_known_args()
    check_if_argument_file_exists(params.on_start)
    check_if_argument_file_exists(params.on_complete)
    check_if_argument_file_exists(params.on_fail)
    check_if_argument_file_exists(params.on_success)
    check_if_argument_file_exists(params.inputs_yaml)
    inputs_yaml = os.path.abspath(params.inputs_yaml)
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
                    print_error("ERROR: Could not find "+ str(workflow_param_value))
                    sys.exit(1)
                workflow_param_value = os.path.abspath(workflow_param_value)
            requirements_dict[workflow_param_key] = (workflow_param_value, parser_option)
    project_id = params.project_id
    restart_job_uuid = params.restart_job_uuid
    inputs_yaml_dirname = os.path.dirname(inputs_yaml)
    inputs_yaml_basename = os.path.basename(inputs_yaml)

    if params.force_overwrite_results and not params.results_dir:
        print_error("ERROR: You need to specify an output directory to force overwrite")
        sys.exit(1)

    work_base_dir = os.path.abspath(pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"])
    # create a new unique job uuid
    if not restart_job_uuid:
        jobstore_uuid = str(uuid.uuid1())
        job_uuid = str(uuid.uuid1())
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
        job_uuid = restart_job_uuid
        restart = True

    work_dir = os.path.join(work_base_dir, job_uuid[:8], job_uuid)
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

    inputs_yaml_work_dir = os.path.join(work_dir,inputs_yaml_basename)
    # convert any relative path in inputs.yaml (e.g. path: ../abc)
    # to absolute path (e.g. path: /ifs/abc)
    convert_yaml_abs_path(inputs_yaml,work_dir,inputs_yaml)
    copy_ignore_same_file(inputs_yaml,inputs_yaml_work_dir)
    #convert_examples_to_use_abs_path(inputs_yaml)

    if project_path:
        input_files_blob = targzip_project_files(project_id, project_path)

    if params.max_mem != parser.get_default('max_mem') or params.max_cpu != parser.get_default('max_cpu'):
        handle_change_max_memory(work_dir,params.max_mem,params.max_cpu,inputs_yaml_work_dir,params.debug_mode)

    # submit
    submit(project_id, job_uuid, params.project_path, pipeline_name, pipeline_version, params.batch_system, params.cwl_batch_system, jobstore_uuid, restart, params.debug_mode, work_dir, params.workflow_name, inputs_yaml_work_dir, pipeline_settings, input_files_blob, params.foreground_mode,requirements_dict, params.test_mode, params.results_dir, params.force_overwrite_results, params.on_start, params.on_complete, params.on_fail, params.on_success, params.use_docker, params.docker_registry, params.max_mem, params.max_cpu)

if __name__ == "__main__":

    main()
