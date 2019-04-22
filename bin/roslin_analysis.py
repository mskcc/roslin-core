#!/usr/bin/env python

import sys
import os
import re
import argparse
import yaml
import copy
import csv
import glob
import uuid
import json
import subprocess
from collections import defaultdict
import shutil
import logging
import redis

logger = logging.getLogger("roslin_analysis")
logger.setLevel(logging.INFO)
redis_host = os.environ.get("ROSLIN_REDIS_HOST")
core_bin_path = os.environ.get("ROSLIN_CORE_BIN_PATH")
redis_port = int(os.environ.get("ROSLIN_REDIS_PORT"))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
roslin_done_path = "/ifs/work/pi/roslin-internal-services/roslin-done.py"

def run_popen(name,command,log_stdout,log_stderr,shell):
    logger.info('---------- Running '+ name + ' ----------')
    logger.info('Args: '+ ' '.join(command))
    single_process = subprocess.Popen(command, stdout=log_stdout,stderr=log_stderr, shell=shell)
    output = None
    error = None
    errorcode = None
    output, error = single_process.communicate()
    errorcode = single_process.returncode
    if errorcode == 0:
        logger.info(name + ' passed')
    else:
        logger.info(name + ' failed with errorcode ' + str(errorcode))
    if output:
        logger.info("----- "+name+" stdout -----\n" + str(output))
    if error:
        logger.info("----- "+name+" stderr -----\n" + str(error))

def run_analysis_helper(params,pipeline_script_path,output_directory,project_name):
    script_name = 'roslin_analysis_helper.py'
    pipeline_script = os.path.join(pipeline_script_path,script_name)
    params_dict = params
    command_args = []
    for single_arg_key in params_dict:
        single_arg = '--'+ single_arg_key
        single_arg_value = params_dict[single_arg_key]
	if single_arg_key != 'disable_portal_repo_update':
		command_args.append(str(single_arg))
		command_args.append(str(single_arg_value))
		if not os.path.exists(single_arg_value):
			error_string = single_arg_value + " does not exist"
			logger.error(error_string)
			sys.exit(1)
	else:
		if single_arg_value == True:
			command_args.append(str(single_arg))
    command = ['python',pipeline_script] + command_args
    run_popen("analysis helper",command,subprocess.PIPE,subprocess.PIPE,False)

def run_roslin_done(project_results_dir,project_id,log_path,disable_portal,disable_jira,enable_slack):
    project_uuid_search = re.search("[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}",project_results_dir)
    project_uuid = None
    if not project_uuid_search:
        logger.error("Project uuid not found")
        sys.exit(1)
    else:
        project_uuid = project_uuid_search.group(0)
    if not os.path.exists(roslin_done_path):
        logger.warning(roslin_done_path + " not found. skipping")
    roslin_done_command = ['python',roslin_done_path]
    if disable_portal:
        roslin_done_command.append('--disable_portal')
    if disable_jira:
        roslin_done_command.append('--disable_jira')
    if not enable_slack:
        roslin_done_command.append('--disable_slack')
    roslin_done_command.extend(['--project_results_dir',project_results_dir,'--project_id',project_id,'--job_uuid',project_uuid,'--log_path',log_path])
    run_popen("roslin done",roslin_done_command,subprocess.PIPE,subprocess.PIPE,False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="convert current project files to yaml input")
    parser.add_argument('--pipeline_bin_path',required=True,help='The path to the pipeline scripts')
    parser.add_argument('--copy_outputs_directory',required=True,help='The output path for roslin copy outputs')
    parser.add_argument('--project_name',required=True,help='The name of the project')
    parser.add_argument('--disable_jira_update',default=False,action='store_true',help='If not updating JIRA, skips portion that comments to JIRA ticket associated with project.')
    parser.add_argument('--disable_portal_repo_update', default=False, action='store_true', help='If not updating cbioportal, skips submitting request to update the Mercurial repo.')
    parser.add_argument('--enable_slack_update', default=False, action='store_true', help='Send project status info to slack')
    params = parser.parse_args()
    disable_jira_update = params.disable_jira_update
    disable_portal_repo_update = params.disable_portal_repo_update
    enable_slack_update = params.enable_slack_update
    disable_slack_update = True
    analysis_helper_args = {}
    output_directory = params.copy_outputs_directory
    log_path = os.path.join(output_directory,'log','roslin_analysis.log')
    log_file_handler = logging.FileHandler(log_path)
    log_file_handler.setLevel(logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s - %(message)s')
    log_file_handler.setFormatter(log_formatter)
    logger.addHandler(log_file_handler)
    if not os.path.exists(output_directory):
        error_string = "The copy outputs directory does not exist"
        logger.error(error_string)
        sys.exit(1)
    portal_output_directory = os.path.join(output_directory,'portal')
    if os.path.exists(portal_output_directory):
        shutil.rmtree(portal_output_directory)
    os.makedirs(portal_output_directory)

    project_name = params.project_name

    clinical_data_file = project_name + '_sample_data_clinical.txt'
    sample_summary_file = project_name + '_SampleSummary.txt'
    request_file = project_name + '_request.txt'

    analysis_helper_args['output_directory'] = portal_output_directory
    analysis_helper_args['clinical_data'] = os.path.join(output_directory,'inputs',clinical_data_file)
    analysis_helper_args['sample_summary'] = os.path.join(output_directory,'qc',sample_summary_file)
    analysis_helper_args['request_file'] = os.path.join(output_directory,request_file)
    analysis_helper_args['roslin_output'] = os.path.join(output_directory,'log','stdout.log')
    analysis_helper_args['maf_directory'] = os.path.join(output_directory,'maf')
    analysis_helper_args['facets_directory'] = os.path.join(output_directory,'facets')
    analysis_helper_args['script_path'] = params.pipeline_bin_path
    analysis_helper_args['disable_portal_repo_update'] = params.disable_portal_repo_update

    run_analysis_helper(analysis_helper_args,params.pipeline_bin_path,output_directory,project_name)
    run_roslin_done(output_directory,project_name,log_path,disable_portal_repo_update,disable_jira_update,enable_slack_update)
