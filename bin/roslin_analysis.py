#!/usr/bin/env python3

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
log_file_handler = logging.FileHandler('roslin_analysis.log')
log_file_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(message)s')
log_file_handler.setFormatter(log_formatter)
logger.addHandler(log_file_handler)

redis_host = os.environ.get("ROSLIN_REDIS_HOST")
core_bin_path = os.environ.get("ROSLIN_CORE_BIN_PATH")
redis_port = int(os.environ.get("ROSLIN_REDIS_PORT"))
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

def publish_jira_update_to_redis(output_directory,project_name,portal_status):
    # connect to redis
    logger.info('------- Sending JIRA update to Redis -------')
    logger.info('project name: ' + project_name)
    logger.info('output directory: ' + output_directory)
    logger.info('portal status: ' + str(portal_status))
    logger.info('core bin path: '+ core_bin_path)
    data = {}
    data['output_directory'] = output_directory
    data['core_bin_path'] = core_bin_path
    data['portal_status'] = str(portal_status)
    data['project_name'] = project_name
    redis_client.publish('roslin-done', json.dumps(data))

def publish_mercurial_update_request_to_redis(project_name):
    logger.info('------- Sending Mercurial update request to Redis -------')
    logger.info('project name: ' + project_name)
    data = dict()
    data['project_name'] = project_name
    redis_client.publish('roslin-mercurial-update', json.dumps(data))

def run_command(params,pipeline_script_path,output_directory,project_name):
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
    logger.info('---------- Running analysis helper ----------')
    logger.info('Script path: ' + pipeline_script)
    logger.info('Args: '+ ' '.join(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    proc.communicate()
    exit_code = proc.wait()
    return exit_code

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="convert current project files to yaml input")
    parser.add_argument('--pipeline_bin_path',required=True,help='The path to the pipeline scripts')
    parser.add_argument('--copy_outputs_directory',required=True,help='The output path for roslin copy outputs')
    parser.add_argument('--project_name',required=True,help='The name of the project')
    parser.add_argument('--disable_jira_update',default=False,action='store_true',help='If not updating JIRA, skips portion that comments to JIRA ticket associated with project.')
    parser.add_argument('--disable_portal_repo_update', default=False, action='store_true', help='If not updating cbioportal, skips submitting request to update the Mercurial repo.')
    params = parser.parse_args()
    disable_jira_update = params.disable_jira_update
    disable_portal_repo_update = params.disable_portal_repo_update
    analysis_helper_args = {}
    output_directory = params.copy_outputs_directory
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

    exit_code = run_command(analysis_helper_args,params.pipeline_bin_path,output_directory,project_name)

    if not disable_jira_update:
        if exit_code == 0:
            publish_jira_update_to_redis(output_directory,project_name,0)
        else:
            publish_jira_update_to_redis(output_directory,project_name,1)
            disable_portal_repo_update = True

    if not disable_portal_repo_update:
         publish_mercurial_update_request_to_redis(project_name)
