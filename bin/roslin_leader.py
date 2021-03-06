#!/usr/bin/env python
from __future__ import print_function
import logging
from toil.common import Toil, safeUnpickleFromStream
from track_utils import log, ReadOnlyFileJobStore, RoslinTrack, get_current_time, add_stream_handler, add_file_handler, log, get_status_names, update_run_results_status, update_workflow_run_results, add_user_event, update_workflow_params
from core_utils import read_pipeline_settings, kill_all_lsf_jobs, check_user_kill_signal, starting_log_message, exiting_log_message, finished_log_message, check_if_argument_file_exists, check_tmp_env, load_yaml, get_common_args, get_leader_args, parse_workflow_args, add_specific_args, get_dir_paths
from toil.common import Toil
from toil.job import Job, JobNode
from toil.leader import FailedJobsException
from toil.batchSystems import registry
from threading import Thread, Event
import time
import os
import json
import traceback
import sys
import signal
import copy
import argparse
import shutil
from functools import partial
from ruamel.yaml import safe_load

status_name_position = get_status_names()
pending_status = status_name_position['pending']
running_status = status_name_position['running']
done_status = status_name_position['done']
exit_status = status_name_position['exit']


def cleanup(clean_up_dict, signal_num, frame):
    logger = clean_up_dict['logger']
    try:
        cleanup_helper(clean_up_dict, signal_num, frame)
    except:
        error_message = "Cleanup failed\n"+str(traceback.format_exc())
        log(logger,'error',error_message)
    finally:
        log(logger,'info',exiting_log_message)
        exit(1)

def cleanup_tmp_files(workflow_params):
    tmp_dir_path = workflow_params['work_dir']
    tmp_dir_list = os.listdir(tmp_dir_path)
    job_store_dir = workflow_params['tmp_dir']
    job_store_name = workflow_params['jobstore'] + '-' + workflow_params['workflow_name']
    job_store_path = os.path.join(job_store_dir,job_store_name)
    if os.path.exists(job_store_path):
        try:
            shutil.rmtree(job_store_path)
        except:
            error_message = "Cleanup failed for: " + str(job_store_path) +"\n"+str(traceback.format_exc())
            log(logger,'error',error_message)
    for single_folder in tmp_dir_list:
        single_folder_path = os.path.join(tmp_dir_path,single_folder)
        if os.path.isdir(single_folder_path):
            try:
                shutil.rmtree(single_folder_path)
            except:
                error_message = "Cleanup failed for: " + str(single_folder_path) +"\n"+str(traceback.format_exc())
                log(logger,'error',error_message)

def cleanup_helper(clean_up_dict, signal_num, frame):
    clean_workflow = clean_up_dict['clean_workflow']
    toil_obj = clean_up_dict['toil_obj']
    track_leader = clean_up_dict['track_leader']
    batch_system =  clean_up_dict['batch_system']
    uuid = clean_up_dict['uuid']
    logger = clean_up_dict['logger']
    workflow = clean_up_dict['workflow']
    signal_name = "Unknown"
    if signal_num == signal.SIGINT:
        signal_name = "SIGINT"
    if signal_num == signal.SIGTERM:
        signal_name = "SIGTERM"
    signal_message = "Received signal: "+ signal_name
    if not clean_workflow.is_set():
        clean_workflow.set()
        project_killed_message = ""
        project_killed_event = {}
        log_dir = workflow_params['log_folder']
        work_dir = workflow_params['project_work_dir']
        tmp_dir = workflow_params['tmp_dir']
        dir_paths = (log_dir,work_dir,tmp_dir)
        user_kill_signal = check_user_kill_signal(workflow.params['project_id'], workflow.params['job_uuid'], workflow.params['pipeline_name'], workflow.params['pipeline_version'], dir_paths=dir_paths)
        if user_kill_signal and 'user_kill_signal' not in clean_up_dict:
            clean_up_dict['user_kill_signal'] = user_kill_signal
        if 'user_kill_signal' in clean_up_dict:
            user_kill_signal_dict = clean_up_dict['user_kill_signal']
            user = user_kill_signal_dict['user']
            hostname = user_kill_signal_dict['hostname']
            time = user_kill_signal_dict['time']
            user_kill_template = "{} has killed this job from {} on {}"
            project_killed_message = user_kill_template.format(user,hostname,time)
            project_killed_event = {"killed_by": "user"}
            project_killed_event.update(user_kill_signal_dict)
        else:
            current_time = get_current_time()
            project_killed_message = "Killed by system [ " + batch_system + " ] on " + current_time
            project_killed_event = {"killed_by": "batch_system", "batch_system": batch_system}
        add_user_event(logger,uuid,project_killed_event,"killed")
        log(logger,"info",project_killed_message)
        batch_system_obj = toil_obj._batchSystem
        if batch_system_obj:
            issued_jobs = batch_system_obj.getIssuedBatchJobIDs()
            job_dict = {}
            if hasattr(batch_system_obj,'jobs'):
                job_dict = batch_system_obj.jobs
            if hasattr(batch_system_obj,'currentJobs'):
                job_dict = batch_system_obj.currentJobs
            for single_issued_job in issued_jobs:
                job_killed_message = "Killing toil job: " + str(single_issued_job)
                if isinstance(job_dict,dict):
                    if single_issued_job in job_dict:
                        job_str = job_dict[single_issued_job]
                        job_name = job_str.split(" ")[1]
                        job_killed_message = job_killed_message + " ( " + str(job_name) + " ) "
                log(logger,"info",job_killed_message)
            batch_system_obj.killBatchJobs(issued_jobs)
        if batch_system == 'LSF':
            kill_all_lsf_jobs(logger,uuid)

def read_file(file_path, file_position):
    if os.path.exists(file_path):
        with open(file_path) as file_obj:
            file_obj.seek(file_position)
            contents = file_obj.read()
            new_position = file_obj.tell()
    else:
        contents = None
        new_position = file_position
    return {'contents':contents,'position':new_position}

def add_version_str(workflow_params):
    roslin_core_version = workflow_params['env']['ROSLIN_CORE_VERSION']
    roslin_pipeline_name = workflow_params['env']['ROSLIN_PIPELINE_NAME']
    roslin_pipeline_version = workflow_params['env']['ROSLIN_PIPELINE_VERSION']
    roslin_cmo_version = workflow_params['env']['ROSLIN_CMO_VERSION']
    version_str = "VERSIONS: roslin-core-{}, roslin-{}-{}, cmo-{}".format(roslin_core_version,roslin_pipeline_name,roslin_pipeline_version,roslin_cmo_version)
    workflow_params['version_str'] = version_str
    return workflow_params

def log_workflow_output(logger,roslin_workflow,job_uuid):
    output_meta_path = roslin_workflow.params["output_meta_json"]
    output_data = load_yaml(output_meta_path)
    workflow_params = copy.deepcopy(roslin_workflow.params)
    workflow_params['outputs'] = output_data
    del workflow_params['requirement_list']
    del workflow_params['logger']
    update_workflow_params(logger,job_uuid,workflow_params)

def workflow_transition(logger,roslin_workflow,job_uuid,status):
    workflow_name = roslin_workflow.params['workflow_name']
    update_run_results_status(logger,job_uuid,status)
    if status == running_status:
        start_message = workflow_name + " is now starting"
        log(logger,'info',start_message)
        roslin_workflow.on_start(logger)
        log(logger,'info',starting_log_message)
    if status == done_status:
        done_message = workflow_name + " is now done"
        log(logger,'info',done_message)
        roslin_workflow.on_success(logger)
        roslin_workflow.on_complete(logger)
        log(logger,'info',"Cleaning up tmp files")
        cleanup_tmp_files(roslin_workflow.params)
        log_workflow_output(logger,roslin_workflow,job_uuid)
        log(logger,'info',finished_log_message)
    if status == exit_status:
        exit_message = workflow_name + " has exited"
        log(logger,'info',exit_message)
        roslin_workflow.on_fail(logger)
        roslin_workflow.on_complete(logger)
        log(logger,'info',exiting_log_message)


def roslin_track(logger,toil_obj,track_leader,job_store_path,job_uuid,clean_up_dict,roslin_workflow,work_dir,tmp_dir):

    def modify_restarted_logging(retry_jobs,log_message,job_name):
        if job_name in retry_jobs:
            log_message = log_message + " ( restarted )"
        return log_message

    workflow_params = roslin_workflow.params
    project_id = workflow_params['project_id']
    pipeline_name = workflow_params['pipeline_name']
    pipeline_version = workflow_params['pipeline_version']
    workflow_log_file = workflow_params['log_file']
    run_attempt = int(workflow_params['run_attempt'])
    roslin_track = RoslinTrack(job_store_path,job_uuid,work_dir,tmp_dir,restart,run_attempt,True,logger)
    #job_store_obj = ReadOnlyFileJobStore(job_store_path)
    leader_job_id = ""
    job_list = []
    done_list = []
    job_info = {}
    retry_failed_jobs = []
    retry_jobs = []
    log_file_positon = 0
    started = False
    job_status = {}
    while track_leader.is_set():
        if toil_obj._batchSystem:
            log_dir = workflow_params['log_folder']
            work_dir = workflow_params['project_work_dir']
            tmp_dir = workflow_params['tmp_dir']
            dir_paths = (log_dir,work_dir,tmp_dir)
            user_kill_signal = check_user_kill_signal(project_id, job_uuid, pipeline_name, pipeline_version, dir_paths)
            if user_kill_signal:
                clean_up_dict['user_kill_signal'] = user_kill_signal
                if 'error_message' in clean_up_dict['user_kill_signal'] and clean_up_dict['user_kill_signal']['error_message'] != None:
                    cleanup(clean_up_dict,None,None)
            new_job_status = roslin_track.check_status_change(job_status,track_leader)
            if new_job_status:
                job_status = copy.deepcopy(new_job_status)
                if not started:
                    workflow_transition(logger,roslin_workflow,job_uuid,running_status)
                    started = True
            log_obj = read_file(workflow_log_file,log_file_positon)
            log_file_positon = log_obj['position']
            log_contents = log_obj['contents'].rstrip()
            if log_contents:
                log(logger_file_monitor,'info',log_contents)
            time.sleep(1)

if __name__ == "__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    parser_project_options = parser.add_argument_group("Roslin project options","Project options")
    common_requirements_list = get_common_args()
    leader_requirements_list = get_leader_args()
    parser_project_options = add_specific_args(parser_project_options,common_requirements_list)
    parser_project_options = add_specific_args(parser_project_options,leader_requirements_list)
    options, other_options = parser.parse_known_args()
    pipeline_name = options.pipeline_name
    pipeline_version = options.pipeline_version
    log_folder = options.log_folder
    project_tmpdir = options.project_tmpdir
    project_workdir = options.project_workdir
    logger = logging.getLogger("roslin_leader")
    logger_file_monitor = logging.getLogger("roslin_leader_monitor")
    pipeline_settings = read_pipeline_settings(pipeline_name, pipeline_version)
    sys.path.append(pipeline_settings['ROSLIN_PIPELINE_BIN_PATH'])
    check_tmp_env(logger)
    import roslin_workflows
    roslin_workflow_class = getattr(roslin_workflows,options.workflow_name)
    workflow_parser = argparse.ArgumentParser(parents=[ parser ], add_help=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    roslin_workflow = roslin_workflow_class(None)
    requirements_obj = roslin_workflow.add_requirement(workflow_parser)
    if requirements_obj:
        workflow_parser, requirements_list = requirements_obj
        workflow_params = workflow_parser.parse_args()
        requirements_dict = parse_workflow_args(workflow_params,requirements_list)
        options = workflow_params
    job_store = options.jobStore
    if options.debug_mode:
        logger.setLevel(logging.DEBUG)
        add_stream_handler(logger,None,logging.DEBUG)
        add_stream_handler(logger_file_monitor,"%(message)s",logging.DEBUG)
        log(logger,"debug","Options:\n")
        log(logger,"debug",options)
        log(logger,"debug","Extra options:\n")
        log(logger,"debug",other_options)
        log(logger,"debug","Workflow specifc options:\n")
        log(logger,"debug",requirements_dict)
    else:
        logger.setLevel(logging.INFO)
        add_stream_handler(logger,None,logging.INFO)
        add_stream_handler(logger_file_monitor,"%(message)s",logging.INFO)
    track_leader = Event()
    clean_workflow = Event()
    clean_workflow.clear()
    if options.run_attempt is not None:
        run_attempt = int(options.run_attempt)
    else:
        run_attempt = 0
    inputs_yaml = options.inputs_yaml
    input_yaml_data = None
    if os.path.exists(inputs_yaml):
        input_yaml_data = load_yaml(inputs_yaml)
        num_pairs = len(input_yaml_data['pairs'])
        num_groups = len(input_yaml_data['groups'])
    process_pid = str(os.getpid())
    work_dir = os.path.abspath(os.path.join(options.project_output,os.pardir))
    pid_file_path = os.path.join(work_dir,'pid')
    with open(pid_file_path,"w") as pid_file:
        pid_file.write(process_pid)
    if 'file:' in job_store:
        job_store = job_store.replace('file:','')
        restart = options.restart
    else:
        log(logger,"error","Only file jobstores are supported")
        exit(1)
    if os.path.exists(job_store) and not restart:
        log(logger,"error","The jobstore already exists, please remove or restart")
        exit(1)
    options.batchSystem = options.batch_system
    options.retryCount = options.retry_count
    options.workDir = project_workdir
    leader_work_dir = os.path.join(project_workdir,'leader')
    max_mem = options.maxMemory
    max_cpu = options.maxCores
    if not os.path.exists(project_tmpdir):
        os.makedirs(project_tmpdir)
    if not os.path.exists(leader_work_dir):
        os.makedirs(leader_work_dir)
    if options.cwl_batch_system:
        cwl_batch_system = options.cwl_batch_system
    else:
        cwl_batch_system = options.batch_system
    if options.use_docker:
        os.environ['ROSLIN_USE_DOCKER'] = "True"
    if options.docker_registry:
        os.environ['DOCKER_REGISTRY_NAME'] = options.docker_registry
    with Toil(options) as toil_obj:
        workflow_failed = False
        workflow_params = {}
        workflow_params_path = os.path.join(project_workdir,"workflow_params.json")
        if os.path.exists(workflow_params_path):
            with open(workflow_params_path,"r") as workflow_params_file:
                workflow_params = json.load(workflow_params_file)
            workflow_params['restart'] = True
        else:
            workflow_params = {'project_id':options.project_id, 'job_uuid':options.project_uuid, 'pipeline_name':options.pipeline_name, 'pipeline_version':options.pipeline_version, 'batch_system':cwl_batch_system, 'jobstore':options.jobstore_uuid, 'restart':restart, 'debug_mode':options.debug_mode, 'output_dir':options.project_output, 'tmp_dir':project_tmpdir, 'workflow_name':options.workflow_name, 'input_yaml':options.inputs_yaml,'log_folder':options.log_folder, 'run_attempt':run_attempt, 'work_dir':project_workdir,'test_mode':options.test_mode,'num_pairs':num_pairs,'num_groups':num_groups, 'project_work_dir':work_dir, 'results_dir':options.project_results, 'force_overwrite_results':options.force_overwrite_results, 'inputs': input_yaml_data, 'workflow_params_path': workflow_params_path, 'on_start': options.on_start, 'on_complete': options.on_complete, 'on_fail': options.on_fail, 'on_success': options.on_success, 'env':dict(os.environ), 'max_mem': max_mem, 'max_cpu':max_cpu}
            workflow_params = add_version_str(workflow_params)
            workflow_params['requirements'] = requirements_dict
        roslin_workflow = roslin_workflow_class(workflow_params)
        clean_up_dict = {'logger':logger,'toil_obj':toil_obj,'track_leader':track_leader,'clean_workflow':clean_workflow,'batch_system':options.batch_system,'uuid':options.project_uuid,'workflow':roslin_workflow}
        signal.signal(signal.SIGINT, partial(cleanup, clean_up_dict))
        signal.signal(signal.SIGTERM, partial(cleanup, clean_up_dict))
        roslin_track = Thread(target=roslin_track, args=([logger, toil_obj, track_leader, job_store, options.project_uuid, clean_up_dict, roslin_workflow, project_workdir, project_tmpdir]))
        roslin_job = roslin_workflow.run_pipeline()
        roslin_workflow_params = copy.deepcopy(roslin_workflow.params)
        del roslin_workflow_params['requirement_list']
        del roslin_workflow_params['logger']
        with open(workflow_params_path,"w") as workflow_params_file:
            json.dump(roslin_workflow_params,workflow_params_file, default=lambda x: None)
        update_workflow_params(logger,options.project_uuid,roslin_workflow_params)
        update_run_results_status(logger,options.project_uuid,pending_status)
        try:
            track_leader.set()
            roslin_track.start()
            if restart:
                toil_obj.restart()
            else:
                toil_obj.start(roslin_job)
        except:
            workflow_failed = True
            log(logger,"error","Workflow failed\n"+str(traceback.format_exc()))
        finally:
            track_leader.clear()
            roslin_track.join()
            if workflow_failed:
                workflow_transition(logger,roslin_workflow,options.project_uuid,exit_status)
                exit(1)
            else:
                workflow_transition(logger,roslin_workflow,options.project_uuid,done_status)
                exit(0)