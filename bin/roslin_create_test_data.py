#!/usr/bin/env python3
import sys
import os
import json
import argparse
import multiprocessing
from multiprocessing.dummy import Pool, current_process
import logging
from subprocess import Popen, PIPE, call
import sys
import os
from queue import Queue
import traceback
import ast
import tempfile
import shutil
import time
import tarfile
import signal
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils import get_choices, create_mpgr, load_pipeline_settings, get_workflow_config
from roslin_submit import get_pipeline_name_and_versions


sys.path.append(os.environ['ROSLIN_PIPELINE_BIN_PATH'])
import roslin_workflows
from track_utils import old_jobs_folder

logger = logging.getLogger("roslin_create_test_data")
logger.propagate = False
logger.setLevel(logging.INFO)
log_file_handler = logging.FileHandler('roslin_create_test_data.log')
log_stream_handler = logging.StreamHandler()
log_file_handler.setLevel(logging.INFO)
log_stream_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(message)s')
log_file_handler.setFormatter(log_formatter)
log_stream_handler.setFormatter(log_formatter)
logger.addHandler(log_file_handler)
logger.addHandler(log_stream_handler)
script_path = os.path.dirname(os.path.realpath(__file__))

def create_test_data(pipeline_settings,num_threads,debug_mode):
	pipeline_name = pipeline_settings['ROSLIN_PIPELINE_NAME']
	pipeline_version = pipeline_settings['ROSLIN_PIPELINE_VERSION']
	pipeline_workspace_path = pipeline_settings['ROSLIN_PIPELINE_WORKSPACE_PATH']
	pipeline_dependency_path = os.path.join(pipeline_workspace_path,'test_data')
	pipeleine_template_path = os.path.join(pipeline_dependency_path,'examples','Proj_ROSLIN_DEV')
	batch_system = 'singleMachine'
	test_data_name = 'roslin_{}_version_{}_test_data'.format(pipeline_name,str(pipeline_version))
	current_dir = os.getcwd()
	dependency_path = os.path.join(current_dir,test_data_name)
	roslin_run_path = os.path.join(current_dir,'roslin_runs')
	if os.path.exists(dependency_path):
		shutil.rmtree(dependency_path)
	if os.path.exists(roslin_run_path):
		shutil.rmtree(roslin_run_path)
	os.mkdir(dependency_path)
	os.mkdir(roslin_run_path)
	examples_folder = os.path.join(dependency_path,'examples')
	meta_folder = os.path.join(dependency_path,'meta')
	data_folder = os.path.join(examples_folder,'data')
	os.makedirs(data_folder)
	os.mkdir(meta_folder)
	fastq_data = os.path.join(pipeline_dependency_path,'examples','data','fastq')
	fastq_folder = os.path.join(data_folder,'fastq')
	template_folder = os.path.join(examples_folder,'Proj_ROSLIN_DEV')
	shutil.copytree(fastq_data,fastq_folder)
	shutil.copytree(pipeleine_template_path,template_folder)
	choices = get_choices()
	workflows = []
	workflow_info = {}
	for single_workflow in choices['workflow_name']:
		workflow_config = get_workflow_config(single_workflow)
		single_dependency_list = workflow_config['params']['workflows'][single_workflow]['single_dependency']
		multi_dependency_list = workflow_config['params']['workflows'][single_workflow]['multi_dependency']
		workflows = workflows + single_dependency_list + multi_dependency_list
		workflows = list(set(workflows))
	for single_workflow in workflows:
		workflow_config = get_workflow_config(single_workflow)
		workflow_dependency_list = workflow_config['params']['workflows'][single_workflow]['dependency_key_list']
		sample_num_list = [None]
		pair_num_list = [None]
		if 'pair_number' in workflow_dependency_list:
			pair_num_list = range(2)
		if 'sample_number' in workflow_dependency_list:
			sample_num_list = range(2)
		for single_pair in pair_num_list:
			for single_sample in sample_num_list:
				workflow_folder = single_workflow
				if single_pair != None:
					workflow_folder = workflow_folder+'Pair'+str(single_pair)
				if single_sample != None:
					workflow_folder = workflow_folder+'Sample'+str(single_sample)
				workflow_path = os.path.join(roslin_run_path,workflow_folder)
				os.mkdir(workflow_path)
				dependency_file_list = create_mpgr(pipeline_name,pipeline_version,single_workflow,batch_system,workflow_path,dependency_path,sample_num=single_sample,pair_num=single_pair,results=None,cwl_batch_system=None,run_args="--test-mode")
				workflow_info[workflow_folder] = dependency_file_list
	run_roslin_parallel(workflow_info,dependency_path,num_threads,debug_mode)

def verbose_logging(single_item):
	if single_item["stdout"]:
		stdout_logging = '---------- STDOUT of '+ single_item["workflow"] + ' ----------\n' + single_item["stdout"]
		logger.info(stdout_logging)
	if single_item["stderr"]:
		stderr_logging = '---------- STDERR of '+ single_item["workflow"] + ' ----------\n' + single_item["stderr"]
		logger.info(stderr_logging)

def run_workflow_wrapper(single_item):
	submission_queue = single_item[0]
	status_queue = single_item[1]
	for workflow_name in iter(submission_queue.get, None):
		try:
			run_output = run_workflow(workflow_name)
			status_queue.put(run_output)
		except:
			error_message = "Error: " + str(workflow_name) + " failed\n " + traceback.format_exc()
			logger.error(error_message)
			output = {'workflow':workflow_name,'stdout':None,'stderr':error_message,'status':1,'name':current_process().name}
			status_queue.put(output)
	output = {'workflow':None,'stdout':None,'stderr':None,'status':0,'name':current_process().name}
	status_queue.put(output)

def run_workflow(workflow_name):
	logger.info("["+current_process().name+"] Running " + workflow_name)
	current_dir = os.getcwd()
	roslin_workflow_path = os.path.join(current_dir,'roslin_runs',workflow_name)
	run_script_path = os.path.join(roslin_workflow_path,'run-example.sh')
	command = [run_script_path]
	process = Popen(command, stdout=PIPE, stderr=PIPE, cwd=roslin_workflow_path)
	stdout, stderr = process.communicate()
	exit_code = process.returncode
	output = {'workflow':workflow_name,'stdout':stdout,'stderr':stderr,'status':exit_code,'name':current_process().name}
	return output

def run_roslin_parallel(workflow_info,dependency_path,num_threads,debug_mode):
	status_queue = Queue()
	submission_queue = Queue()
	meta_path = os.path.join(dependency_path,'meta')
	total_number_of_jobs = len(workflow_info.keys())
	total_processed = 0
	current_dir = os.getcwd()
	job_list = []
	pending_list = []
	finished_list = []
	for single_workflow in  workflow_info:
		dependency_list = workflow_info[single_workflow]
		if len(dependency_list) == 0:
			submission_queue.put(single_workflow)
		else:
			pending_list.append(single_workflow)
	job_list = [(submission_queue,status_queue)] * num_threads
	original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
	pool = Pool(num_threads)
	signal.signal(signal.SIGINT, original_sigint_handler)
	try:
		run_results = pool.map_async(run_workflow_wrapper,job_list)
		set_sentinel = False
		while run_results.ready() == False and set_sentinel == False:
			single_item = status_queue.get()
			thread_name = single_item['name']
			workflow_name = single_item['workflow']
			if single_item['status'] == 0:
				if workflow_name == None:
					continue
				else:
					total_processed = total_processed + 1
					logger.info("["+thread_name+"] " + workflow_name + " finished ( " + str(total_processed) + "/"+str(total_number_of_jobs)+" )")
					roslin_submission_path = os.path.join(current_dir,'roslin_runs',workflow_name,"submission.json")
					with open(roslin_submission_path,"r") as submission_file:
						submission_data = json.load(submission_file)
					output_dir = submission_data['project_output_dir']
					output_meta_path = os.path.join(output_dir,'output-meta.json')
					dependency_meta_path = os.path.join(meta_path,workflow_name)
					dependency_output_path = os.path.join(dependency_path,'examples','data',workflow_name)
					os.mkdir(dependency_output_path)
					for single_item in os.listdir(output_dir):
					    item_path = os.path.join(output_dir,single_item)
					    if os.path.isdir(item_path):
					        if single_item != old_jobs_folder:
					        	dependency_data_path = os.path.join(dependency_output_path,single_item)
					        	shutil.copytree(item_path,dependency_data_path)
					output_meta_data = ''
					with open(output_meta_path,'r') as output_meta_file:
						output_meta_data = output_meta_file.read()
					dependency_output_meta = output_meta_data.replace(output_dir,dependency_output_path)
					with open(dependency_meta_path,'w') as dependency_meta_file:
						dependency_meta_file.write(dependency_output_meta)
					if debug_mode == True:
						verbose_logging(single_item)
					finished_list.append(workflow_name)
					submitted_list = []
					for single_workflow in pending_list:
						dependency_finished = True
						for single_dependency in workflow_info[single_workflow]:
							if single_dependency not in finished_list:
								dependency_finished = False
						if dependency_finished:
							submission_queue.put(single_workflow)
							submitted_list.append(single_workflow)
					for single_workflow in submitted_list:
						pending_list.remove(single_workflow)
			else:
				status_message = "["+thread_name+"] " + workflow_name + " failed"
				verbose_logging(single_item)
				logger.error(status_message)
				pool.terminate()
				sys.exit(status_message)
			if total_processed==total_number_of_jobs and not set_sentinel:
				sentinel_list = [None] * num_threads
				for single_sentinel in sentinel_list:
					submission_queue.put(single_sentinel)
				set_sentinel = True
	except KeyboardInterrupt:
        exit_message = "Keyboard interrupt, terminating workers"
        logger.info(exit_message)
        pool.terminate()
        sys.exit(exit_message)
	logger.info("---------- Finished running roslin workflows ----------")
	data_path = os.path.join(dependency_path,'examples','data')
	for single_item in os.listdir(meta_path):
		item_path = os.path.join(meta_path,single_item)
		template_path = item_path + '.template'
		if os.path.isfile(item_path):
			with open(item_path,'r') as meta_file:
				meta_data = meta_file.read()
			template_data = meta_data.replace(data_path,'{{ data_path }}')
			with open(template_path,'w') as template_file:
				template_file.write(template_data)
			os.remove(item_path)
	dependency_name = os.path.basename(dependency_path) + ".tar.gz"
	with tarfile.open(dependency_name,"w:gz") as tar:
		tar.add(dependency_path,arcname=os.path.sep)







def main():
	"main function"

	parser = argparse.ArgumentParser(description='roslin create test data', add_help=False)

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

	parser.add_argument(
		"--threads",
		action="store",
		dest="threads",
		help="The number of roslin workflows to run in parallel",
		default=4,
		required=False
	)

	parser.add_argument(
		"--debug",
		action="store_true",
		dest="debug_mode",
		help="Run in debug mode",
		required=False
	)

	params, _ = parser.parse_known_args()
	pipeline_name = params.pipeline_name
	pipeline_version = params.pipeline_version
	threads = int(params.threads)
	debug_mode = params.debug_mode
	# load the Roslin Pipeline settings
	pipeline_settings = load_pipeline_settings(pipeline_name, pipeline_version)
	create_test_data(pipeline_settings,threads,debug_mode)

if __name__ == "__main__":

	main()
