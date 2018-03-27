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

# create logger
logger = logging.getLogger("roslin_portal")
logger.setLevel(logging.INFO)
# create the file handler
log_file_handler = logging.FileHandler('roslin_portal.log')
log_file_handler.setLevel(logging.INFO)
# create a logging format
log_formatter = logging.Formatter('%(asctime)s - %(message)s')
log_file_handler.setFormatter(log_formatter)
# add the handlers to the logger
logger.addHandler(log_file_handler)

def run_command(params,pipeline_script_path):
    script_name = 'roslin_portal_helper.py'    
    pipeline_script = os.path.join(pipeline_script_path,script_name)
    params_dict = params
    command_args = []
    for single_arg_key in params_dict:
        single_arg = '--'+ single_arg_key
        single_arg_value = params_dict[single_arg_key]
        command_args.append(single_arg)
        command_args.append(single_arg_value)
        if not os.path.exists(single_arg_value):
            error_string = single_arg_value + " does not exist"
            logger.error(error_string)
            sys.exit(1)
    command = ['python',pipeline_script] + command_args
    logger.info('---------- Running portal helper ----------')
    logger.info('Script path: ' + pipeline_script)
    logger.info('Args: '+ ' '.join(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="convert current project files to yaml input")
    parser.add_argument('--pipeline_bin_path',required=True,help='The path to the pipeline scripts')
    parser.add_argument('--copy_outputs_directory',required=True,help='The output path for roslin copy outputs')
    parser.add_argument('--project_name',required=True,help='The name of the project')
    params = parser.parse_args()
    portal_helper_args = {}
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

    portal_helper_args['output_directory'] = portal_output_directory
    portal_helper_args['clinical_data'] = os.path.join(output_directory,'inputs',clinical_data_file)
    portal_helper_args['sample_summary'] = os.path.join(output_directory,'qc',sample_summary_file)
    portal_helper_args['request_file'] = os.path.join(output_directory,request_file)
    portal_helper_args['roslin_output'] = os.path.join(output_directory,'log','stdout.log')
    portal_helper_args['maf_directory'] = os.path.join(output_directory,'maf')
    portal_helper_args['facets_directory'] = os.path.join(output_directory,'facets')
    portal_helper_args['script_path'] = params.pipeline_bin_path
   
    run_command(portal_helper_args,params.pipeline_bin_path)
