#!/usr/bin/env python3
import sys
import os
import json
import argparse
import logging
import sys
import os
from queue import Queue
import traceback
import ast
import tempfile
import shutil
import time
import tarfile
import requests
from clint.textui import progress
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import get_choices, create_mpgr, load_pipeline_settings, get_workflow_config, read_test_settings, get_template, write_to_disk, copy_ignore_same_file
from roslin_submit import get_pipeline_name_and_versions

sys.path.append(os.environ['ROSLIN_PIPELINE_BIN_PATH'])
import roslin_workflows

logger = logging.getLogger("roslin_create_test_files")
logger.propagate = False
logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler('roslin_create_test_files.log')
log_stream_handler = logging.StreamHandler()
log_file_handler.setLevel(logging.INFO)
log_stream_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(message)s')
log_file_handler.setFormatter(log_formatter)
log_stream_handler.setFormatter(log_formatter)
logger.addHandler(log_file_handler)
logger.addHandler(log_stream_handler)
script_path = os.path.dirname(os.path.realpath(__file__))

def download_test_data(test_data_path,test_data_url,test_data_archive):
	test_data_tar_path = test_data_path + '.tar'
	if not os.path.exists(test_data_archive):
		logger.info("Downloading test data")
		test_data_request = requests.get(test_data_url, stream=True)
		with open(test_data_tar_path,'wb') as test_data_tar:
			total_length = int(test_data_request.headers.get('content-length'))
			for chunk in progress.bar(test_data_request.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1):
			    if chunk:
			        test_data_tar.write(chunk)
			        test_data_tar.flush()
	else:
		logger.info("Using test data from archive: "+str(test_data_archive))
		copy_ignore_same_file(test_data_archive,test_data_tar_path)
	test_data_tar_file = tarfile.open(test_data_tar_path)
	os.mkdir(test_data_path)
	test_data_tar_file.extractall(path=test_data_path)

def create_meta_files(test_data_path):
	logger.info("Creating meta files")
	template_path = os.path.join(test_data_path,'meta')
	data_path = os.path.join(test_data_path,'examples','data')
	for single_template in os.listdir(template_path):
		single_template_path = os.path.join(template_path,single_template)
		if os.path.isfile(single_template_path):
			single_template_output_path = single_template_path.replace('.template','')
			single_template = get_template(single_template_path)
			single_template_data = single_template.render(data_path=data_path)
			write_to_disk(single_template_output_path,single_template_data)

def create_test_data(pipeline_settings,test_settings):
	pipeline_name = pipeline_settings['ROSLIN_PIPELINE_NAME']
	pipeline_version = pipeline_settings['ROSLIN_PIPELINE_VERSION']
	workflow_path = pipeline_settings['ROSLIN_EXAMPLE_PATH']
	test_data_path = os.path.join(pipeline_settings['ROSLIN_PIPELINE_WORKSPACE_PATH'],'test_data')
	test_data_url = test_settings['ROSLIN_TEST_DATA_URL']
	test_data_archive = test_settings['ROSLIN_TEST_DATA_PATH']
	if not os.path.exists(test_data_path):
		download_test_data(test_data_path,test_data_url,test_data_archive)
		create_meta_files(test_data_path)
	run_args = test_settings['ROSLIN_TEST_RUN_ARGS']
	batch_system = test_settings['ROSLIN_TEST_BATCHSYSTEM']
	cwl_batch_system = test_settings['ROSLIN_TEST_CWL_BATCHSYSTEM']
	if os.path.exists(workflow_path):
		shutil.rmtree(workflow_path)
	os.mkdir(workflow_path)
	choices = get_choices()
	workflows = choices['workflow_name']
	workflow_info = {}
	for single_workflow in workflows:
		logger.info("Creating mpgr files for workflow: " + str(single_workflow))
		single_workflow_path = os.path.join(workflow_path,single_workflow)
		os.mkdir(single_workflow_path)
		dependency_file_list = create_mpgr(pipeline_name,pipeline_version,single_workflow,batch_system,single_workflow_path,test_data_path,results=None,cwl_batch_system=cwl_batch_system,run_args=run_args)

def main():
	"main function"

	parser = argparse.ArgumentParser(description='roslin create test files', add_help=False)

	pipeline_name_and_version = get_pipeline_name_and_versions()

	parser.add_argument(
		"--name",
		action="store",
		dest="pipeline_name",
		help=pipeline_name_and_version['pipeline_name_help'],
		choices=pipeline_name_and_version['pipeline_name_choices'],
		required=True
	)

	parser.add_argument(
		"--version",
		action="store",
		dest="pipeline_version",
		help=pipeline_name_and_version['pipeline_version_help'],
		choices=pipeline_name_and_version['pipeline_version_choices'],
		required=True
	)

	params, _ = parser.parse_known_args()
	pipeline_name = params.pipeline_name
	pipeline_version = params.pipeline_version
	# load the Roslin Pipeline settings
	pipeline_settings = load_pipeline_settings(pipeline_name, pipeline_version)
	test_settings = read_test_settings(pipeline_name, pipeline_version)
	create_test_data(pipeline_settings,test_settings)

if __name__ == "__main__":

	main()