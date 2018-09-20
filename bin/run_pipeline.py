#!/usr/bin/env python

from __future__ import print_function
from shutil import copyfile
import os, sys, glob, uuid, argparse, subprocess, glob
import hashlib
import datetime
import json
import tarfile
import base64
import time
import re
import pytz
import redis
import subprocess
import shlex
import yaml

import json2yaml


class WorkflowJob:
    def __init__(self, project_id, job_uuid, pipeline_name_version, batch_system, restart_jobstore_uuid, debug_mode, output_dir, work_dir, workflow_name, input_yaml_file): 
        self.project_id = project_id
        self.job_uuid = job_uuid
        self.pipeline_name_version = pipeline_name_version
        self.batch_system = batch_system
        self.restart_jobstore_uuid = restart_jobstore_uuid
        self.debug_mode = debug_mode
        self.output_dir = output_dir
        self.workflow_name = workflow_name
        self.work_dir = work_dir
        self.input_yaml_file = input_yaml_file
        self.output_dir = output_dir
        self.output_meta_json = self.output_dir + os.sep + 'output-meta.json'
        
    def build_workflow_command(self):
        if self.pipeline_name_version != None:
            job_command = "roslin-runner.sh -v {} -w {} -i {} -b {} -p {} -j {} -o {}".format(
                self.pipeline_name_version,
                self.workflow_name,
                self.input_yaml_file,
                self.batch_system,
                self.project_id,
                self.job_uuid,
                self.output_dir
            )
        else:
            job_command = "roslin-runner.sh -w {} -i {} -b {} -p {} -j {} -o {}".format(
                self.workflow_name,
                self.input_yaml_file,
                self.batch_system,
                self.project_id,
                self.job_uuid,
                self.output_dir
            )
    
        # add "-r" if restart jobstore uuid is supplied
        if self.restart_jobstore_uuid:
            job_command = job_command + " -r {}".format(self.restart_jobstore_uuid)
    
        # add "-d" if debug_mode is turned on
        if self.debug_mode:
            job_command = job_command + " -d"
    
        return job_command 
 
    def run_workflow(self):
        job_command = self.build_workflow_command()
        # TODO: add check if this workflow was previously completed; if complete, return 0    
        args = shlex.split(job_command) 
        print("Running workflow %s" % self.workflow_name)
        print("Output directory: %s" % self.output_dir)
        print("Running command %s" % job_command)
        self.process_run = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def already_exist(self):
        if len(self.get_tmp_directories()) == 0:
            return True
        return False

    def communicate(self):
        print("Waiting for %s to complete..." % self.workflow_name)
        return self.process_run.communicate()

    def poll(self):
        print("Polling %s... " % self.workflow_name)
        return self.process_run.poll()

    def get_tmp_directories(self):
        tmp_dirs = glob.glob(self.output_dir + os.sep + "tmp*")
        out_tmp_dirs =  glob.glob(self.output_dir + os.sep + "out_tmpdir*")
        result = tmp_dirs + out_tmp_dirs
        return result

    def get_output_json(self):
        return self.output_meta_json


# Submits a workflow job subprocess; returns the process workflow_job itself, which can be .communicate() or .poll()
def submit_process(params, input_yaml_file, workflow_name, output_dir_name):
    project_id = params.project_id
    job_uuid = params.job_uuid
    pipeline_name_version = params.pipeline_name_version
    batch_system = params.batch_system
    restart_jobstore_uuid = params.restart_jobstore_uuid
    debug_mode = params.debug_mode
    work_dir = params.work_dir
    output_dir = os.path.join(work_dir, output_dir_name)
   
    workflow_job = WorkflowJob(project_id, job_uuid, pipeline_name_version, batch_system, restart_jobstore_uuid, debug_mode, output_dir, work_dir, workflow_name, input_yaml_file)
    workflow_job.run_workflow()
    return workflow_job

def delete_tmp_dirs(workflow_jobs):
    for workflow_job in workflow_jobs:
        tmp_dirs_list = workflow_job.get_tmp_directories()
        for tmp_dir in tmp_dirs_list:
            print("Should delete tmp_dir: %s" % tmp_dir)


def will_run_sv(request_file_path):
    with open(request_file_path, 'rb') as request_data:
        for line in request_data:
            data = line.split(":")
            if len(data) > 1:
                if data[0] == "Assay":
                    if "IMPACT" in data[1] or "HemePACT" in data[1]:
                        return True
    return False


def execute_run_pipeline(params):
    # create a new unique job uuid if not supplied
    params.job_uuid = params.job_uuid if params.job_uuid is not None else str(uuid.uuid1())

    # must be one of the singularity binding points
    work_dir = params.work_dir

    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    input_yaml = params.input_yaml_file

    # determine if we're doing find_svs by looking through request_file
    tmp_yaml_data = yaml.load(open(input_yaml, 'rb'))
    request_file_path = tmp_yaml_data['db_files']['request_file']['path']
    find_sv = will_run_sv(request_file_path)

    # Add XENOGRAFT stuff here

    # Prototyping this for now
    alignment_process = submit_process(params, input_yaml, "alignment.cwl", "alignment")
    a_stdout, a_stderr = alignment_process.communicate() #wait for alignment to complete
    running_processes = [ alignment_process ]

    if alignment_process.poll() == 1:
        print("Alignment broke; exiting")
        print(a_stdout)
        print(a_stderr)
        sys.exit(1)

    print("Alignment done.")

    alignment_json = alignment_process.get_output_json()

    post_alignment_path = os.path.join(params.work_dir, "post-alignment.yaml")
    post_alignment_yaml = json2yaml.create_roslin_yaml(alignment_json, post_alignment_path, input_yaml)

    gather_metrics_process = submit_process(params, post_alignment_yaml, "gather_metrics.cwl", "gather_metrics")
    time.sleep(5)
    conpair_process = submit_process(params, post_alignment_yaml, "conpair.cwl", "conpair")
    time.sleep(5)

    running_processes.append(gather_metrics_process)
    running_processes.append(conpair_process)

    # add find_svs conditional
    if find_sv:
        find_svs_process = submit_process(params, post_alignment_yaml, "find_svs.cwl", "find_svs")
        running_processes.append(find_svs_process)
    time.sleep(5)

    variant_calling_process = submit_process(params, post_alignment_yaml, "variant_calling.cwl","variant_calling")
    time.sleep(5)
    v_stdout, v_stderr = variant_calling_process.communicate() # wait for variant_calling to finish
    
    if variant_calling_process.poll() == 1:
        print("Variant Calling broke; can't do filtering. Check.")
        print(v_stdout)
        print(v_stderr)
        sys.exit(1)

    variant_calling_json = variant_calling_process.get_output_json()
    post_variant_calling_path = os.path.join(params.work_dir, "post-variant-calling.yaml")
    post_variant_calling_yaml = json2yaml.create_roslin_yaml(variant_calling_json, post_variant_calling_path,post_alignment_yaml)

    filtering_process = submit_process(params, post_variant_calling_yaml, "filtering.cwl", "filtering")
    time.sleep(5)

    running_processes.append(filtering_process) 
    
    # Checks for successful processes
    num_processes = len(running_processes)
    failed_processes = set()
    successful_processes = set()
    while len(failed_processes) + len(successful_processes) < num_processes:
        for running_process in running_processes:
            cwl_name = running_process.workflow_name
            returncode = running_process.poll()
            if running_process not in successful_processes or running_process not in failed_processes:
                if returncode == 1:
                    if running_process not in failed_processes:
                        print("FAILED: " + cwl_name)
                        failed_processes.add(running_process)
                if returncode == 0:
                    if running_process not in successful_processes:
                        print("SUCCESS: " + cwl_name)
                        successful_processes.add(running_process)
            time.sleep(.5)
        time.sleep(5)

    num_success = len(successful_processes)
    num_fail = len(failed_processes)

    print("Summary")
    print("Total workflows: %i" % (num_success + num_fail))
    print("Num success: %i" % num_success)
    print("Num fail: %i" % num_fail)

    delete_tmp_dirs(successful_processes)


if __name__ == "__main__":
    "main function"

    parser = argparse.ArgumentParser(description='submit')

    parser.add_argument("--id", action="store", dest="project_id", help="CMO Project ID (e.g. Proj_5088_B)", required=True)
    parser.add_argument("--pipeline", action="store", dest="pipeline_name_version", help="Pipeline name/version (e.g. variant/2.4.0)", required=True)
    parser.add_argument("--restart", action="store", dest="restart_jobstore_uuid", help="jobstore uuid for restart", required=False)
    parser.add_argument("--batch-system", action="store", dest="batch_system", default="lsf", choices=["lsf", "singleMachine", "mezos"],
            help="Which version of roslin-runner to run (lsf, singleMachine, or mezos)", required=False)
    parser.add_argument("--debug", action="store_true", dest="debug_mode", help="Run the runner in debug mode")
    parser.add_argument("--input-yaml", action="store", dest="input_yaml_file", default="inputs.yaml", help="The input YAML file")
    parser.add_argument("--job-uuid", action="store", dest="job_uuid", default=None, help="Existing job UUID")
    parser.add_argument("--work-dir", action="store", dest="work_dir", default=".", help="Working directory for project.")

    params = parser.parse_args()
    print("Current working directory: " + os.getcwd())
    execute_run_pipeline(params)
