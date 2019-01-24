#!/usr/bin/env python
from __future__ import print_function
from toil.common import Toil, safeUnpickleFromStream
from track_utils import log, ReadOnlyFileJobStore, RoslinTrack, ProjectWorkflow, get_current_time, add_stream_handler, add_file_handler, log, get_status_names, update_run_results_status, update_workflow_run_results, add_user_event
from core_utils import read_pipeline_settings, kill_all_lsf_jobs, check_user_kill_signal
from toil.common import Toil
from toil.job import Job, JobNode
from toil.leader import FailedJobsException
from toil.batchSystems import registry
from threading import Thread, Event
import time
import os
import logging
import json
import traceback
import sys
import signal
import copy
import argparse
from functools import partial

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
        exit(1)

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
            job_dict = batch_system_obj.jobs
            for single_issued_job in issued_jobs:
                job_str = job_dict[single_issued_job]
                job_name = job_str.split(" ")[1]
                job_killed_message = "Killing job: " + str(job_name)
                log(logger,"info",job_killed_message)
            batch_system_obj.killBatchJobs(issued_jobs)
        if batch_system == 'LSF':
            kill_all_lsf_jobs(logger,uuid)
        workflow_transition(logger,workflow,uuid,exit_status)

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


def workflow_transition(logger,roslin_workflow,job_uuid,status):
    workflow_name = roslin_workflow.params['workflow_name']
    update_run_results_status(logger,job_uuid,status)
    if status == running_status:
        start_message = workflow_name + " is now starting"
        log(logger,'info',start_message)
        roslin_workflow.on_start()
    if status == done_status:
        done_message = workflow_name + " is now done"
        log(logger,'info',done_message)
        roslin_workflow.on_success()
        roslin_workflow.on_complete()
    if status == exit_status:
        exit_message = workflow_name + " has exited"
        log(logger,'info',exit_message)
        roslin_workflow.on_fail()
        roslin_workflow.on_complete()


def roslin_track(logger,toil_obj,track_leader,job_store_path,job_uuid,clean_up_dict,roslin_workflow,work_dir,tmp_dir):

    def modify_restarted_logging(retry_jobs,log_message,job_name):
        if job_name in retry_jobs:
            log_message = log_message + " ( restarted )"
        return log_message

    workflow_params = roslin_workflow.params
    project_id = workflow_params['project_id']
    pipeline_name = workflow_params['pipeline_name']
    pipeline_version = workflow_params['pipeline_version']
    run_attempt = int(workflow_params['run_attempt'])
    roslin_track = RoslinTrack(job_store_path,job_uuid,work_dir,tmp_dir,restart,run_attempt,logger)
    #job_store_obj = ReadOnlyFileJobStore(job_store_path)
    leader_job_id = ""
    job_list = []
    done_list = []
    job_info = {}
    retry_failed_jobs = []
    retry_jobs = []
    workflow_log_file = roslin_workflow.log_file
    log_file_positon = 0
    started = False
    job_status = {}
    while track_leader.is_set():
        if toil_obj._batchSystem:
            user_kill_signal = check_user_kill_signal(project_id, job_uuid, pipeline_name, pipeline_version)
            if user_kill_signal:
                clean_up_dict['user_kill_signal'] = user_kill_signal
                if 'error_message' in clean_up_dict and clean_up_dict['error_message'] != None:
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
    parser_project_options.add_argument(
        "--id",
        action="store",
        dest="project_id",
        help="Project ID (e.g. Proj_5088_B)",
        required=True
    )
    parser_project_options.add_argument(
        "--jobstore-id",
        action="store",
        dest="jobstore_uuid",
        help="The uuid of the jobstore",
        required=True
    )
    parser_project_options.add_argument(
        "--debug_mode",
        action="store_true",
        dest="debug_mode",
        help="Run the runner in debug mode"
    )
    parser_project_options.add_argument(
        "--uuid",
        action="store",
        dest="project_uuid",
        help="The uuid of the project",
        required=True
    )
    parser_project_options.add_argument(
        "--inputs",
        action="store",
        dest="inputs_yaml",
        help="The path to your input yaml file (e.g. /ifs/projects/CMO/Proj_5088_B/inputs.yaml)",
        required=True
    )

    parser_project_options.add_argument(
        "--project-output",
        action="store",
        dest="project_output",
        help="Path to Project output",
        required=True
    )

    parser_project_options.add_argument(
        "--log-folder",
        action="store",
        dest="log_folder",
        help="Path to folder to store the logs",
        required=True
    )

    parser_project_options.add_argument(
        "--project-workdir",
        action="store",
        dest="project_workdir",
        help="Path to Project workdir",
        required=True
    )

    parser_project_options.add_argument(
        "--project-tmpDir",
        action="store",
        dest="project_tmpdir",
        help="Path to Project tmpdir",
        required=True
    )

    parser_project_options.add_argument(
        "--workflow",
        action="store",
        dest="workflow_name",
        help="Workflow name (e.g. project-workflow)",
        required=True
    )

    parser_project_options.add_argument(
        "--pipeline-name",
        action="store",
        dest="pipeline_name",
        help="Pipeline name (e.g. variant)",
        required=True
    )

    parser_project_options.add_argument(
        "--pipeline-version",
        action="store",
        dest="pipeline_version",
        help="Pipeline version (e.g. 2.4.0)",
        required=True
    )

    parser_project_options.add_argument(
        "--batch-system",
        action="store",
        dest="batch_system",
        choices=list(registry._UNIQUE_NAME),
        help="The batch system to submit the job",
        default="singleMachine",
        required=True
    )

    parser_project_options.add_argument(
        "--cwl-batch-system",
        action="store",
        dest="cwl_batch_system",
        choices=list(registry._UNIQUE_NAME),
        help="The batch system to submit the cwl jobs (uses --batch-system if not set)",
        required=False
    )

    parser_project_options.add_argument(
        "--run-attempt",
        action="store",
        dest="run_attempt",
        help="Number of times the run has been ateempted, used to id the run when restarting the job",
        required=False
    )
    parser_project_options.add_argument(
        "--retry-count",
        action="store",
        dest="retry_count",
        help="Number of times the piepline can retry failed jobs",
        required=True
    )

    options, _ = parser.parse_known_args()
    pipeline_name = options.pipeline_name
    pipeline_version = options.pipeline_version
    log_folder = options.log_folder
    project_tmpdir = options.project_tmpdir
    project_workdir = options.project_workdir
    logger = logging.getLogger("roslin_leader")
    logger_file_monitor = logging.getLogger("roslin_leader_monitor")
    pipeline_settings = read_pipeline_settings(pipeline_name, pipeline_version)
    sys.path.append(pipeline_settings['ROSLIN_PIPELINE_BIN_PATH'])
    import roslin_workflows
    roslin_workflow_class = getattr(roslin_workflows,options.workflow_name)
    workflow_parser = argparse.ArgumentParser(parents=[ parser ], add_help=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    roslin_workflow = roslin_workflow_class(None)
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
            requirements_dict[workflow_param_key] = workflow_param_value
        options = workflow_params
    job_store = options.jobStore
    if options.debug_mode:
        logger.setLevel(logging.DEBUG)
        add_stream_handler(logger,None,logging.DEBUG)
        add_stream_handler(logger_file_monitor,"%(message)s",logging.DEBUG)
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
    if not os.path.exists(project_tmpdir):
        os.makedirs(project_tmpdir)
    if not os.path.exists(leader_work_dir):
        os.makedirs(leader_work_dir)
    if options.cwl_batch_system:
        cwl_batch_system = options.cwl_batch_system
    else:
        cwl_batch_system = options.batch_system
    with Toil(options) as toil_obj:
        workflow_failed = False
        workflow_params = {'project_id':options.project_id, 'job_uuid':options.project_uuid, 'pipeline_name':options.pipeline_name, 'pipeline_version':options.pipeline_version, 'batch_system':cwl_batch_system, 'jobstore':options.jobstore_uuid, 'restart':restart, 'debug_mode':options.debug_mode, 'output_dir':options.project_output, 'tmp_dir':project_tmpdir, 'workflow_name':options.workflow_name, 'input_yaml':options.inputs_yaml,'log_folder':options.log_folder, 'run_attempt':run_attempt, 'work_dir':project_workdir}
        workflow_params.update(requirements_dict)
        roslin_workflow = roslin_workflow_class(workflow_params)
        clean_up_dict = {'logger':logger,'toil_obj':toil_obj,'track_leader':track_leader,'clean_workflow':clean_workflow,'batch_system':options.batch_system,'uuid':options.project_uuid,'workflow':roslin_workflow}
        signal.signal(signal.SIGINT, partial(cleanup, clean_up_dict))
        signal.signal(signal.SIGTERM, partial(cleanup, clean_up_dict))
        roslin_track = Thread(target=roslin_track, args=([logger, toil_obj, track_leader, job_store, options.project_uuid, clean_up_dict, roslin_workflow, project_workdir, project_tmpdir]))
        roslin_job = roslin_workflow.run_pipeline()
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
                log(logger,"error","exiting")
                exit(1)
            else:
                workflow_transition(logger,roslin_workflow,options.project_uuid,done_status)
                exit(0)