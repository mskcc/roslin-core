from __future__ import print_function
import os
import shutil
import logging
logging.getLogger("rdflib").setLevel(logging.WARNING)
logging.getLogger("toil.jobStores.fileJobStore").setLevel(logging.WARNING)
logging.getLogger("toil.jobStores.abstractJobStore").disabled = True
logging.getLogger("toil.toilState").setLevel(logging.WARNING)
from toil.common import Toil, safeUnpickleFromStream
from toil.jobStores.fileJobStore import FileJobStore
from toil.toilState import ToilState as toil_state
from toil.job import Job
from threading import Thread, Event
from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
from toil.job import JobException
from toil.jobStores.abstractJobStore import NoSuchJobStoreException, NoSuchFileException
import pickle
import re
from string import punctuation
import datetime
import time
import copy
from subprocess import PIPE, Popen
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from core_utils import read_pipeline_settings, run_command, print_error, create_roslin_yaml, convert_yaml_abs_path, check_if_env_is_empty
import dill
import simplejson
import sys
import yaml
import copy
import traceback
import re

time_format="%Y-%m-%d %H:%M:%S"
log_format="(%(current_time)s) [%(name)s:%(levelname)s] %(message)s"
termination_file_name = "killed-by-user.json"
submission_file_name = "submitted-by-user.json"
old_jobs_folder = "oldJobs"
DOC_VERSION = "1.0.0"
MONGO_HOST = os.environ['ROSLIN_MONGO_HOST']
MONGO_PORT = os.environ['ROSLIN_MONGO_PORT']
MONGO_DATABASE = os.environ['ROSLIN_MONGO_DATABASE']
MONGO_USERNAME = os.environ['ROSLIN_MONGO_USERNAME']
MONGO_PASSWORD = os.environ['ROSLIN_MONGO_PASSWORD']
mongo_username_and_password = ''
if check_if_env_is_empty(MONGO_USERNAME):
	mongo_username_and_password = str(MONGO_USERNAME)
if check_if_env_is_empty(MONGO_PASSWORD):
	mongo_username_and_password = mongo_username_and_password + ":" + str(MONGO_PASSWORD)
if mongo_username_and_password:
	mongo_username_and_password = mongo_username_and_password + "@"
MONGO_URL = 'mongodb://' + mongo_username_and_password + str(MONGO_HOST) + ':' + str(MONGO_PORT)
index_key = 'pipelineJobId'
RUN_RESULTS_COLLECTION = "RunResults"
PROJECTS_COLLECTION = "Projects"
RUN_PROFILES_COLLECTION = "RunProfiles"
RUN_DATA_COLLECTION = "RunData"
client = MongoClient(MONGO_URL, connect=False)

### mongo wrappers ###

def get_mongo_collection(logger,collection_name):
	if client:
		try:
			db = client[MONGO_DATABASE]
			db[collection_name].create_index(index_key)
			return db[collection_name]
		except ConnectionFailure:
			error_message = "Failed to get collection "+ collection_name
			log(logger,"error",error_message)

def get_mongo_document(logger,collection_name,project_uuid):
	if client:
		try:
			db, single_doc_query = get_db_and_doc_query(logger,collection_name,project_uuid)
			single_doc = db[collection_name].find_one(single_doc_query)
			return single_doc
		except ConnectionFailure:
			error_message = "Failed to update mongo document for project [ " + project_uuid + " ] to collection "+ collection_name
			log(logger,"error",error_message)

def update_mongo_document(logger,collection_name,project_uuid,updated_document):
	if client and isinstance(updated_document, dict):
		if updated_document:
			mongo_safe_dict = make_mongo_safe_dict(logger,updated_document)
			try:
				mongo_doc_id = None
				db, single_doc_query = get_db_and_doc_query(logger,collection_name,project_uuid)
				single_doc = get_mongo_document(logger,collection_name,project_uuid)
				if mongo_safe_dict:
					if single_doc == None:
						mongo_doc_id = db[collection_name].replace_one(filter=single_doc_query,upsert=True,replacement=mongo_safe_dict)
					else:
						mongo_doc_id = db[collection_name].update_one(filter=single_doc_query, update={"$set":mongo_safe_dict})
				return mongo_doc_id
			except ConnectionFailure:
				error_message = "Failed to connect and update mongo document for project [ " + project_uuid + " ] to collection "+ collection_name
				log(logger,"error",error_message)
			except:
				error_message = "Failed to update mongo document for project [ " + project_uuid + " ] to collection "+ collection_name + "\n" + str(mongo_safe_dict) + "\n" + traceback.format_exc()
				log(logger,"error",error_message)
				sys.exit(1)

def make_mongo_key_safe(key):
	return key.replace("$","").replace(".","")

def make_mongo_safe_tuple(logger,tuple_obj):
	list_value = list(tuple_obj)
	return tuple(make_mongo_safe_list(logger,list_value))

def make_mongo_safe_list(logger,list_obj):
	mongo_safe_list = []
	safe_list_elem = None
	for single_item in list_obj:
		if isinstance(single_item, dict):
			safe_list_elem = make_mongo_safe_dict(logger,single_item)
		elif isinstance(single_item, tuple):
			safe_list_elem = make_mongo_safe_tuple(logger,single_item)
		elif isinstance(single_item, list):
			safe_list_elem = make_mongo_safe_list(logger,single_item)
		else:
			if isinstance(single_item, (str, unicode, float, int, bool)):
				safe_list_elem = single_item
		mongo_safe_list.append(safe_list_elem)
	return mongo_safe_list

def make_mongo_safe_dict(logger,dict_obj):
	keys_to_delete = []
	updated_dict = {}
	for single_key in dict_obj:
		if single_key == '_id':
			continue
		mongo_safe_key = make_mongo_key_safe(single_key)
		dict_value = dict_obj[single_key]
		current_key = single_key
		if mongo_safe_key != single_key:
			current_key = mongo_safe_key
			keys_to_delete.append(single_key)
		if isinstance(dict_value, dict):
			updated_dict[current_key] = make_mongo_safe_dict(logger,dict_value)
		elif isinstance(dict_value, tuple):
			updated_dict[current_key] = make_mongo_safe_tuple(logger,dict_value)
		elif isinstance(dict_value, list):
			updated_dict[current_key] = make_mongo_safe_list(logger,dict_value)
		else:
			if isinstance(dict_value, (str, unicode, float, int, bool)):
				updated_dict[current_key] = dict_value
			else:
				updated_dict[current_key] = None
	for single_key_to_delete in keys_to_delete:
		del dict_obj[single_key_to_delete]
	if updated_dict:
		dict_obj.update(updated_dict)
	return dict_obj

def get_db_and_doc_query(logger,collection_name,project_uuid):
	db = client[MONGO_DATABASE]
	collection = get_mongo_collection(logger,collection_name)
	single_doc_id = project_uuid
	single_doc_query = {index_key: single_doc_id}
	return db,single_doc_query

### logging wrappers ###

def add_stream_handler(logger,stream_format,logging_level):
	if not stream_format:
		formatter = logging.Formatter(log_format)
	else:
		formatter = logging.Formatter(stream_format)
	logger.propagate = False
	stream_handler = logging.StreamHandler()
	stream_handler.setLevel(logging_level)
	stream_handler.setFormatter(formatter)
	logger.addHandler(stream_handler)

def add_file_handler(logger,file_path,file_format,logging_level):
	if not file_format:
		formatter = logging.Formatter(log_format)
	else:
		formatter = logging.Formatter(file_format)
	logger.propagate = False
	file_handler = logging.FileHandler(file_path)
	file_handler.setLevel(logging_level)
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)

def log(logger,log_level,message):
	current_time = get_current_time()
	if not logger:
		if 'error' in log_level:
			print_error(message)
		else:
			print(message)
	try:
		logging_function = getattr(logger,log_level)
		logging_function(str(message), extra={'current_time':str(current_time)})
	except AttributeError:
		logger.error("Log Level: "+ str(log_level) + " not found.\nOriginal message: "+ str(message), extra={'current_time':str(current_time)})

### time wrappers ###

def get_current_time():
	current_time = datetime.datetime.now().strftime(time_format)
	return current_time

def get_time_difference(first_time,second_time):
	first_time_obj = datetime.datetime.strptime(first_time,time_format)
	second_time_obj = datetime.datetime.strptime(second_time,time_format)
	time_delta =  first_time_obj - second_time_obj
	total_seconds = time_delta.total_seconds()
	minute_seconds = 60
	hour_seconds = 3600
	day_seconds = 86400
	days = divmod(total_seconds,day_seconds)
	hours = divmod(days[1],hour_seconds)
	minutes = divmod(hours[1],minute_seconds)
	seconds = minutes[1]
	days_abs = abs(int(days[0]))
	hours_abs = abs(int(hours[0]))
	minutes_abs = abs(int(minutes[0]))
	seconds_abs = abs(int(seconds))
	total_seconds_abs = abs(int(total_seconds))
	time_difference = {'days':days_abs,'hours':hours_abs,'minutes':minutes_abs,'seconds':seconds_abs,'total_seconds':total_seconds_abs}
	return time_difference


def get_time_difference_from_now(time_obj_str):
	current_time_str = get_current_time()
	time_difference = get_time_difference(current_time_str,time_obj_str)
	return time_difference

def time_difference_to_string(time_difference,max_number_of_time_units):
	number_of_time_units = 0
	time_difference_string = ""
	time_unit_list = ['day(s)','hour(s)','minute(s)','second(s)']
	for single_time_unit in time_unit_list:
		if time_difference[single_time_unit] != 0 and max_number_of_time_units > number_of_time_units:
			time_difference_string = time_difference_string + str(time_difference[single_time_unit]) + " " + str(single_time_unit) + " "
			number_of_time_units = number_of_time_units + 1
	if not time_difference_string:
		return "0 seconds "
	return time_difference_string

def track_job(track_job_flag,params,job_params,restart,logger):
	try:
		track_job_helper(track_job_flag,params,job_params,restart,logger)
	except:
		error_message = "Tracker failed.\n"+traceback.format_exc()
		log(logger,"error",error_message)
		sys.exit(error_message)


def track_job_helper(track_job_flag,params,job_params,restart,logger):
	job_store_id = job_params['jobstore']
	work_dir = job_params['work_dir']
	tmp_dir = job_params['tmp_dir']
	job_store_path = os.path.join(tmp_dir,job_store_id)
	project_uuid = params['job_uuid']
	run_attempt = params['run_attempt']
	full_job_store_path = os.path.join(tmp_dir,job_store_path)
	poll_interval = job_params['poll_interval']
	roslin_track = RoslinTrack(job_store_path,project_uuid,work_dir,tmp_dir,restart,run_attempt,logger)
	roslin_track.check_status(poll_interval,track_job_flag)


def update_batch_system_run_results(logger,project_uuid,status_change,job_dict):
	run_result_doc = get_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid)
	run_data_doc = get_mongo_document(logger,RUN_DATA_COLLECTION,project_uuid)
	if not run_result_doc or not run_data_doc:
		return
	for single_job_key in status_change:
		single_job_obj = status_change[single_job_key]['job_obj']
		single_tool_status = single_job_obj['single_tool_status']
		job_name = single_job_obj['job_name']
		job_id = single_job_obj['job_id']
		status = single_job_obj['status']
		tool_status = job_dict[job_name]
		job_id, job_doc = construct_job_doc(tool_status,job_name,job_id,status)
		job_info = job_doc.pop("jobInfo")
		run_data_doc['jobData'][job_id] = job_info
		run_result_doc['batchSystemJobs'][job_id] = job_doc
	run_result_doc['timestamp']['lastUpdated'] = get_current_time()
	update_mongo_document(logger,RUN_DATA_COLLECTION,project_uuid,run_data_doc)
	update_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid,run_result_doc)

def update_workflow_run_results(logger,project_uuid,workflow_jobs_dict):
	run_result_doc = get_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid)
	if not run_result_doc:
		return
	for single_workflow_job_key in workflow_jobs_dict:
		single_workflow_job = workflow_jobs_dict[single_workflow_job_key]
		job_id, job_doc = construct_workflow_run_results_doc(single_workflow_job)
		run_result_doc['workflowJobs'][job_id] = job_doc
	run_result_doc['timestamp']['lastUpdated'] = get_current_time()
	update_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid,run_result_doc)

def modify_all_running_or_pending_jobs(job_dict,updated_status):
	status_name_dict = get_status_names()
	for job_id in job_dict:
		job_obj = job_dict[job_id]
		job_status = job_obj['status']
		if job_status == status_name_dict['running'] or job_status == status_name_dict['pending'] or job_status == status_name_dict['unknown']:
			job_obj['status'] = status_name_dict[updated_status]
			job_obj['timestamp']['finished'] = get_current_time()

def fail_all_running_or_pending_jobs(job_dict):
	modify_all_running_or_pending_jobs(job_dict,'exit')

def finish_all_running_or_pending_jobs(job_dict):
	modify_all_running_or_pending_jobs(job_dict,'done')

def update_run_result_doc(logger,project_uuid,run_result_doc):
	update_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid,run_result_doc)

def update_project_doc(logger,project_uuid,project_doc):
	update_mongo_document(logger,PROJECTS_COLLECTION,project_uuid,project_doc)

def update_run_data_doc(logger,project_uuid,run_data_doc):
	update_mongo_document(logger,RUN_DATA_COLLECTION,project_uuid,run_data_doc)

def update_run_results_status(logger,project_uuid,status):
	run_result_doc = get_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid)
	if not run_result_doc:
		return
	status_name_dict = get_status_names()
	current_time = get_current_time()
	duration = None
	if status == status_name_dict['exit']:
		fail_all_running_or_pending_jobs(run_result_doc['batchSystemJobs'])
		fail_all_running_or_pending_jobs(run_result_doc['workflowJobs'])
	if status == status_name_dict['done']:
		finish_all_running_or_pending_jobs(run_result_doc['batchSystemJobs'])
		finish_all_running_or_pending_jobs(run_result_doc['workflowJobs'])
	if status == status_name_dict['running']:
		if run_result_doc["timestamp"]["started"] == None:
			run_result_doc["timestamp"]["started"] = current_time
		started_time = run_result_doc["timestamp"]["started"]
		duration = get_time_difference_from_now(started_time)['total_seconds']
	if status == status_name_dict['done'] or status == status_name_dict['exit']:
		if run_result_doc["timestamp"]["finished"] == None:
			run_result_doc["timestamp"]["finished"] = current_time
			started_time = run_result_doc["timestamp"]["started"]
			finished_time = current_time
			#duration = get_time_difference(started_time, finished_time)['total_seconds']
	run_result_doc["timestamp"]["duration"] = duration
	run_result_doc['status'] = status
	run_result_doc['timestamp']['lastUpdated'] = get_current_time()
	update_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid,run_result_doc)

def update_run_results_restart(logger,project_uuid,submitted_time):
	status_name_dict = get_status_names()
	run_result_doc = get_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid)
	if not run_result_doc:
		return
	restart_dict = {'status':run_result_doc['status'],'timestamp':run_result_doc['timestamp']}
	run_result_doc['restarts'].append(restart_dict)
	run_result_doc['status'] = status_name_dict['pending']
	run_result_doc['timestamp'] = { "started": None, "finished": None, "submitted": submitted_time, "lastUpdated": get_current_time(), "duration": None }
	update_mongo_document(logger,RUN_RESULTS_COLLECTION,project_uuid,run_result_doc)

def update_latest_project(logger,project_uuid,project_id):
	latest_project = get_mongo_document(logger,PROJECTS_COLLECTION,"latest_project")
	if not latest_project:
		latest_project = {}
	safe_key = make_mongo_key_safe(project_id)
	latest_project[safe_key] = project_uuid
	update_mongo_document(logger,PROJECTS_COLLECTION,"latest_project",latest_project)

def construct_workflow_run_results_doc(single_job_info):
	name = single_job_info['job_name']
	job_id = single_job_info['job_id']
	status = single_job_info['status']
	submitted_time = single_job_info['submitted_time']
	started_time = single_job_info['started_time']
	finished_time = single_job_info['finished_time']
	duration = None
	job_params = single_job_info['job_params']
	if started_time and finished_time:
		duration = get_time_difference(started_time, finished_time)['total_seconds']
	else:
		if started_time:
			duration = get_time_difference_from_now(started_time)['total_seconds']
	workflow_run_result = {
		"name": name,
		"status": status,
		"memory": job_params['memory'],
		"disk": job_params['disk'],
		"cores": job_params['cores'],
		"jobInfo": None,
		"logFile": None,
		"restarts": [],
		"timestamp": {
			"started": started_time,
			"finished": finished_time,
			"submitted": submitted_time,
			"lastUpdated": get_current_time(),
			"duration": duration
		}


	}

	mongo_safe_job_id = make_mongo_key_safe(job_id)
	return (mongo_safe_job_id, workflow_run_result)

def add_user_event(logger, project_uuid, user_event_data, user_event_type):
	project_doc = get_mongo_document(logger,PROJECTS_COLLECTION,project_uuid)
	if not project_doc:
		return
	current_time = get_current_time()
	user_event = {"type":user_event_type, "time":current_time, "data":user_event_data}
	project_doc["userEvents"].append(user_event)
	update_mongo_document(logger,PROJECTS_COLLECTION,project_uuid,project_doc)


def construct_run_results_doc(pipeline_name, pipeline_version, project_id, project_path, job_uuid, jobstore_uuid, work_dir, workflow, input_files, user_id, submitted_time, cwltoil_log, stdout_log, stderr_log):
	status_name_dict = get_status_names()
	pending_status = status_name_dict['pending']
	run_result = {
		"docVersion": DOC_VERSION,
		"pipelineJobId":job_uuid,
		"pipelineJobStoreId":jobstore_uuid,
		"pipelineVersion": pipeline_version,
		"projectId": project_id,
		"workflow": workflow,
        "workDir": work_dir,
        "userId": user_id,
        "labels": [],
        "timestamp": {
        	"started": None,
        	"finished": None,
        	"submitted": submitted_time,
        	"lastUpdated": get_current_time(),
        	"duration": None
        },
        "status": pending_status,
        "logFiles": {
        	"cwltoil": cwltoil_log,
        	"stdout": stdout_log,
        	"stderr": stderr_log
        },
        "batchSystemJobs": {},
        "workflowJobs": {},
        "outputs": {}
	}

	return run_result

def construct_run_data_doc(job_uuid,jobstore_uuid,pipeline_version,project_id):
	run_data = {
		"docVersion": DOC_VERSION,
		"pipelineJobId":job_uuid,
		"pipelineJobStoreId":jobstore_uuid,
		"pipelineVersion": pipeline_version,
		"projectId": project_id,
		"jobData": {}
	}
	return run_data

def construct_project_doc(logger,pipeline_name, pipeline_version, project_id, project_path, job_uuid, jobstore_uuid, work_dir, workflow, input_files, restart):

	project_collection = get_mongo_collection(logger,PROJECTS_COLLECTION)
	previous_projects = project_collection.find({'projectId':project_id})
	previous_runs = []
	for single_project in previous_projects:
		previous_runs.append(single_project['pipelineJobId'])

	project = {
		"docVersion": DOC_VERSION,
        "projectId": project_id,
        "pipelineJobId": job_uuid,
        "jobstoreId": jobstore_uuid,
        "workflow": workflow,
        "dateSubmitted": get_current_time(),
        "pipelineName": pipeline_name,
        "pipelineVersion": pipeline_version,
        "restart": restart,
        "inputFiles": input_files,
        "userEvents": [],
        "previousRuns": previous_runs
    }

	return project

def get_status_names():
	status_name_dict = {'running':'RUN','pending':'PEND','done':'DONE','exit':'EXIT','unknown':'UNKWN'}
	return status_name_dict

def construct_job_doc(single_tool_status,job_name,job_id,status):

	worker_obj = single_tool_status['workers'][job_id]
	submitted_dict = single_tool_status['submitted']
	done_dict = single_tool_status['done']
	exit_dict = single_tool_status['exit']
	started_time = None
	finished_time = None
	submitted_time = None
	last_updated = None
	duration = None
	job_info = None
	if "started" in worker_obj:
		started_time = worker_obj["started"]
	if "last_modified" in worker_obj:
		last_updated = worker_obj["last_modified"]
	else:
		last_updated = get_current_time()
	if job_id in done_dict:
		finished_time = done_dict[job_id]
	if job_id in  submitted_dict:
		submitted_time = submitted_dict[job_id]
	if job_id in exit_dict:
		finished_time = exit_dict[job_id]
	if 'job_info' in worker_obj:
		job_info = worker_obj['job_info']
	if started_time and finished_time:
		duration = get_time_difference(started_time, finished_time)['total_seconds']
	else:
		if started_time:
			duration = get_time_difference_from_now(started_time)['total_seconds']
		if finished_time:
			started_time = finished_time
			duration = get_time_difference(started_time, finished_time)['total_seconds']
	job_doc = {
		"name": job_name,
		"status": status,
		"memory": worker_obj['memory'],
		"disk": worker_obj['disk'],
		"cores": worker_obj['cores'],
		"jobInfo": job_info,
		"logFile": None,
		"timestamp": {
			"started": started_time,
			"finished": finished_time,
			"submitted": submitted_time,
			"lastUpdated": last_updated,
			"duration": duration
		}

	}

	mongo_safe_job_id = make_mongo_key_safe(job_id)

	return (mongo_safe_job_id, job_doc)

class RoslinJob(Job):
	def __init__(self, job_function, params,job_params):
		memory = job_params['memory']
		cores = job_params['cores']
		disk = job_params['disk']
		Job.__init__(self,  memory=memory, cores=int(cores), disk=disk)
		self.compressed_job_function = dill.dumps(job_function)
		self.params = params
		self.job_params = job_params

	def run(self, fileStore):
		pipeline_name = self.params['pipeline_name']
		pipeline_version  = self.params['pipeline_version']
		pipeline_settings = read_pipeline_settings(pipeline_name, pipeline_version)
		sys.path.append(pipeline_settings['ROSLIN_PIPELINE_BIN_PATH'])
		job_function = dill.loads(self.compressed_job_function)
		job_name = self.job_params['name']
		logger = dill.loads(self.params['logger'])
		try:
			return_code = job_function(self.params,self.job_params)
			if return_code!=0 and return_code!=None:
				error_message = "Job " + str(job_name) + " failed. Returned: "+str(return_code)+"\n"
				log(logger,"info",error_message)
				sys.exit(error_message)
		except Exception:
			error_message = "Job " + str(job_name) + " failed.\n"+traceback.format_exc()
			log(logger,"error",error_message)
			sys.exit(error_message)

def find_unique_name_in_dir(root_name,directory):
	current_num = 1
	found_unique_name = False
	unique_name = ""
	new_name = root_name
	while not found_unique_name:
		current_path = os.path.join(directory,new_name)
		if os.path.exists(current_path):
			new_name = root_name + str(current_num)
			current_num = current_num + 1
		else:
			found_unique_name = True
			unique_name = new_name
	return new_name

def find_unique_name_in_dict(root_name,dict_obj):
	current_num = 1
	found_unique_key = False
	unique_key = ""
	new_key = root_name
	while not found_unique_key:
		if new_key in dict_obj:
			new_key = root_name + str(current_num)
			current_num = current_num + 1
		else:
			found_unique_key = True
			unique_key = new_key
	return new_key

class RoslinWorkflow():
	def __init__(self,params):
		if not params:
			return
		output_dir = params['output_dir']
		params['output_meta_json'] = os.path.join(output_dir,'output-meta.json')
		workflow_name = params['workflow_name']
		log_folder = params['log_folder']
		if 'logger' not in params:
			if params['debug_mode']:
				logging_level = logging.DEBUG
			else:
				logging_level = logging.INFO
			logger = logging.getLogger(workflow_name)
			log_file = workflow_name +".log"
			log_path = os.path.join(log_folder,log_file)
			logger.setLevel(logging_level)
			add_file_handler(logger,log_path,None,logging_level)
			params['logger'] = dill.dumps(logger)
			self.log_file = log_path
		self.jobs = {}
		self.params = params
		self.configure()

	def configure(self):
		pass

	def add_requirement(self,parser):
		pass

	def set_default_job_params(self):
		job_params = {}
		job_params['input_yaml'] = self.params['input_yaml']
		job_params['batch_system'] = self.params['batch_system']
		job_params['tmp_dir'] = self.params['tmp_dir']
		job_params['poll_interval'] = 2
		job_params['memory'] = '2G'
		job_params['cores'] = 1
		job_params['disk'] = '3G'
		job_params['restart'] = False
		job_params['parent_output_meta_json'] = None
		return job_params

	def create_workflow(self,parent_job_params_list,input_yaml,cwl_name,job_name):
		workflow_params = self.params
		job_params = self.set_default_job_params()
		workflow_output_directory = workflow_params['output_dir']
		workflow_job = None
		if parent_job_params_list:
			parent_input_yaml_list = []
			parent_output_meta_json_list = []
			for single_parent_params in parent_job_params_list:
				parent_input_yaml = single_parent_params['input_yaml']
				parent_output_meta_json = single_parent_params['output_meta_json']
				parent_input_yaml_list.append(parent_input_yaml)
				parent_output_meta_json_list.append(parent_output_meta_json)
			job_params['parent_output_meta_json_list'] = parent_output_meta_json_list
			job_params['parent_input_yaml_list'] = parent_input_yaml_list
		if input_yaml:
			job_params['input_yaml'] = input_yaml
		if cwl_name:
			job_params['cwl'] = cwl_name
		if parent_job_params_list:
			workflow_job = self.create_job(self.get_input_yaml_from_job,workflow_params,job_params,job_name)
		else:
			workflow_job = self.create_job(self.run_cwl,workflow_params,job_params,job_name)
		return (workflow_job,job_params)

	def create_job(self,function,params,job_params,name):
		jobs_dict = self.jobs
		workflow_output_directory = self.params['output_dir']
		current_name = find_unique_name_in_dict(name,jobs_dict)
		job_params['jobstore'] = params['jobstore'] + "-" + current_name
		job_output_dir = os.path.join(workflow_output_directory,current_name)
		job_work_dir = os.path.join(self.params['work_dir'],current_name)
		job_params['work_dir'] = job_work_dir
		job_params['output_dir'] = job_output_dir
		job_params['output_meta_json'] = os.path.join(job_output_dir,'output-meta.json')
		job_params['name'] = current_name
		jobs_dict[current_name] = job_params
		roslin_job_obj = RoslinJob(function,params,job_params)
		roslin_job_obj.__dict__['jobName'] = current_name
		return roslin_job_obj

	def get_input_yaml_from_job(self,params,job_params):
		output_meta_json_list = job_params['parent_output_meta_json_list']
		yaml_location = job_params['input_yaml']
		yaml_list = []
		if 'parent_input_yaml_list' in job_params:
			yaml_list = job_params['parent_input_yaml_list']
		else:
			yaml_list.append(params['input_yaml'])

		create_roslin_yaml(output_meta_json_list,yaml_location,yaml_list)
		return 0

	def run_cwl(self,params,job_params):
		logger = dill.loads(params['logger'])
		project_id = params['project_id']
		job_uuid = params['job_uuid']
		pipeline_name_version = params['pipeline_name'] + "/" + params['pipeline_version']
		debug_mode = params['debug_mode']
		test_mode = params['test_mode']
		batch_system = job_params['batch_system']
		job_work_dir = job_params['work_dir']
		job_tmp_dir = job_params['tmp_dir']
		job_output_dir = job_params['output_dir']
		workflow_output_directory = params['output_dir']
		job_yaml = job_params['input_yaml']
		job_cwl = job_params['cwl']
		job_jobstore = job_params['jobstore']
		job_restart = job_params['restart']
		job_name = job_params['name']
		log_folder = params['log_folder']
		job_store_name = job_jobstore
		job_store_path = os.path.join(job_tmp_dir,job_store_name)
		job_suffix = ''
		if os.path.exists(job_store_path):
			job_restart = True
		if os.path.exists(job_output_dir):
			job_error_folder = os.path.join(workflow_output_directory,old_jobs_folder)
			if not os.path.exists(job_error_folder):
				os.mkdir(job_error_folder)
			folder_basename = os.path.basename(job_output_dir)
			new_folder_basename = find_unique_name_in_dir(folder_basename,job_error_folder)
			new_error_folder = os.path.join(job_error_folder,new_folder_basename)
			shutil.move(job_output_dir,new_error_folder)
		input_yaml_basename = os.path.dirname(params['input_yaml'])
		convert_yaml_abs_path(job_yaml,input_yaml_basename,job_yaml)
		roslin_runner_command = ["roslin-runner.sh",
		"-v",pipeline_name_version,
		"-w",job_cwl,
		"-i",job_yaml,
		"-b",batch_system,
		"-j",job_jobstore,
		"-k",job_work_dir,
		"-u",job_uuid,
		"-o",job_output_dir]
		if job_restart:
			roslin_runner_command.extend(["-r"])
		if debug_mode:
			roslin_runner_command.extend(["-d"])
		if test_mode:
			roslin_runner_command.extend(["-t"])
		if not os.path.exists(log_folder):
			os.mkdir(log_folder)
		log_stdout = job_name + "-stdout.log"
		log_stderr = job_name + "-stderr.log"
		log_path_stdout = os.path.join(log_folder,log_stdout)
		log_path_stderr = os.path.join(log_folder,log_stderr)
		if os.path.exists(log_path_stdout) or os.path.exists(log_path_stderr):
			log_error_folder = os.path.join(log_folder,old_jobs_folder)
			if not os.path.exists(log_error_folder):
				os.mkdir(log_error_folder)
			if os.path.exists(log_path_stdout):
				archive_log_stdout = find_unique_name_in_dir(log_stdout,log_error_folder)
				log_failed_stdout = os.path.join(log_error_folder,archive_log_stdout)
				shutil.move(log_path_stdout,log_failed_stdout)
			if os.path.exists(log_path_stderr):
				archive_log_stderr = find_unique_name_in_dir(log_stderr,log_error_folder)
				log_failed_stderr = os.path.join(log_error_folder,archive_log_stderr)
				shutil.move(log_path_stderr,log_failed_stderr)
		track_job_flag = Event()
		roslin_track_worker = Thread(target=track_job, args=(track_job_flag,params,job_params,job_restart,logger))
		track_job_flag.set()
		roslin_track_worker.start()
		cwl_process_ret_code = run_command(roslin_runner_command,log_path_stdout,log_path_stderr,False,True)
		track_job_flag.clear()
		roslin_track_worker.join()
		error_code = cwl_process_ret_code['errorcode']
		return error_code

	def add_requirement(self,parser):
		pass
	def on_start(self):
		pass
	def run_pipeline(self):
		pass
	def on_complete(self):
		pass
	def on_fail(self):
		pass
	def on_success(self):
		pass

class ProjectWorkflow(RoslinWorkflow):
	def run_pipeline(self):
		default_job_params = self.set_default_job_params()
		default_job_params['cwl'] = "project-workflow.cwl"
		leader_job = self.create_job(self.run_cwl,self.params,default_job_params,"ProjectWorkflow")
		return leader_job

class ReadOnlyFileJobStore(FileJobStore):

	def __init__(self, path):
		super(ReadOnlyFileJobStore,self).__init__(path)
		self.failed_jobs = []
		#this assumes we start toil with retryCount=1
		self.default_retry_count = 1
		self.retry_jobs = []
		self.job_cache = {}
		self.job_store_path = path
		self.logger = None

	def check_if_job_exists(self,job_store_id):
		try:
			self._checkJobStoreId(job_store_id)
			return True
		except:
			return False

	def load(self,job_store_id):
		self._checkJobStoreId(job_store_id)
		if job_store_id in self.job_cache:
			return self.job_cache[job_store_id]
		job_file = self._getJobFileName(job_store_id)
		with open(job_file, 'rb') as file_handle:
			job = pickle.load(file_handle)
		return job

	def setJobCache(self):
		job_cache = {}
		for single_job in self.jobs():
			job_id = single_job.jobStoreID
			job_cache[job_id] = single_job
			if single_job.logJobStoreFileID != None:
				failed_job = copy.deepcopy(single_job)
				self.failed_jobs.append(failed_job)
			if single_job.remainingRetryCount == self.default_retry_count:
				retry_job = copy.deepcopy(single_job)
				self.retry_jobs.append(retry_job)
		self.job_cache = job_cache

	def getFailedJobs(self):
		return self.failed_jobs

	def getRestartedJobs(self):
		return self.retry_jobs

	def getFullLogPath(self,logPath):
		job_store_path = self.job_store_path
		full_path = os.path.join(job_store_path,'tmp',logPath)
		return full_path

	def writeFile(self, localFilePath, jobStoreID=None):
		pass

	def update(self, job):
		job_id = job.jobStoreID
		self.job_cache[job_id] = job

	def updateFile(self, jobStoreFileID, localFilePath):
		pass

	def delete(self,job_store_id):
		del self.job_cache[job_store_id]

	def deleteFile(self, jobStoreFileID):
		pass

	def robust_rmtree(self, path, max_retries=3):
		pass

	def destroy(self):
		pass

	def create(self, jobNode):
		pass

	def _writeToUrl(cls, readable, url):
		pass

	def writeFile(self, localFilePath, jobStoreID=None):
		pass

	def writeFileStream(self, jobStoreID=None):
		pass

	def writeSharedFileStream(self, sharedFileName, isProtected=None):
		pass

	def writeStatsAndLogging(self, statsAndLoggingString):
		pass

	def readStatsAndLogging(self, callback, readAll=False):
		pass

	def _getTempSharedDir(self):
		pass

	def _getTempFile(self, jobStoreID=None):
		pass


class RoslinTrack():

	def __init__(self,job_store_path,project_uuid,work_dir,tmp_dir,restart,run_attempt,logger):
		self.job_store_path = job_store_path
		self.work_dir = work_dir
		self.tmp_dir = tmp_dir
		self.jobs_path = {}
		self.run_attempt = run_attempt
		self.workflow_id = ''
		self.jobs = {}
		self.retry_job_ids = {}
		self.current_jobs = []
		self.failed_jobs = []
		self.worker_jobs = {}
		self.project_uuid = project_uuid
		self.job_store_obj = None
		self.job_store_resume_attempts = 5000
		self.restart = restart
		self.restart_num = 2
		self.logger = logger

	def create_job_id(self,jobStoreID,remainingRetryCount):
		logger = self.logger
		restart = self.restart
		restart_num =  self.restart_num
		retry_job_ids = self.retry_job_ids
		run_attempt = int(self.run_attempt)
		first_run_attempt = run_attempt
		second_run_attempt = run_attempt + 1
		id_suffix = None
		id_prefix = None
		if restart:
			id_prefix = str(second_run_attempt) + '-'
		else:
			id_prefix = str(first_run_attempt) + '-'
		if remainingRetryCount == restart_num:
			id_suffix = '-0'
		else:
			if jobStoreID not in retry_job_ids:
				id_suffix = '-0'
			else:
				id_suffix = '-1'
		id_string = id_prefix + str(jobStoreID) + id_suffix
		return id_string

	def resume_job_store(self):
		logger = self.logger
		job_store_path = self.job_store_path
		read_only_job_store_obj = ReadOnlyFileJobStore(job_store_path)
		read_only_job_store_obj.resume()
		read_only_job_store_obj.setJobCache()
		job_store_cache = read_only_job_store_obj.job_cache
		try:
			root_job = read_only_job_store_obj.clean(jobCache=job_store_cache)
		except:
			retry_message = "Could not clean job store from jobCache, retrying"
			root_job = read_only_job_store_obj.clean()
			log(logger,'debug',retry_message)
		self.job_store_obj = read_only_job_store_obj
		return {"job_store":read_only_job_store_obj,"root_job":root_job}

	def get_file_modification_time(self,file_path):
		if os.path.exists(file_path):
			last_modified_epoch = os.path.getmtime(file_path)
			last_modified = datetime.datetime.fromtimestamp(last_modified_epoch).strftime(time_format)
		else:
			last_modified = get_current_time()
		return last_modified

	def mark_job_as_failed(self,job_id,job_name):
		failed_job_list = self.failed_jobs
		job_dict = self.jobs
		current_time = get_current_time()
		if job_id not in failed_job_list:
			failed_job_list.append(job_id)
			if job_name not in CWL_INTERNAL_JOBS:
				job_key = self.make_key_from_file(job_name,True)
				if job_key in job_dict:
					tool_dict = job_dict[job_key]
					if job_id in tool_dict['submitted']:
						tool_dict['exit'][job_id] = current_time
						if job_id in tool_dict['done']:
							del tool_dict['done'][job_id]

	def check_jobs(self,track_job_flag):
		logger = self.logger
		job_dict = self.jobs
		jobs_path = self.jobs_path
		job_store_resume_attempts = self.job_store_resume_attempts
		retry_job_ids = self.retry_job_ids
		current_attempt = 0
		job_store_obj = None
		while not job_store_obj and track_job_flag.is_set():
			try:
				job_store_obj = self.resume_job_store()
			except:
				retry_message = "Jobstore not created yet, trying again ( Attempt " +str(current_attempt) + " / " + str(job_store_resume_attempts) + " )"
				log(logger,"debug",retry_message)
				if current_attempt < job_store_resume_attempts:
					current_attempt = current_attempt + 1
					time.sleep(5)
				else:
					error_message = "Jobstore failed to create, check the workflow logs.\n"+traceback.format_exc()
					log(logger,"error",error_message)
					sys.exit(error_message)
		if not track_job_flag.is_set() or not job_store_obj:
			return
		current_jobs = []
		job_store = job_store_obj["job_store"]
		root_job = job_store_obj["root_job"]
		job_store_cache = job_store.job_cache
		self.workflow_id = job_store.config.workflowID
		toil_state_obj = None
		current_attempt = 0
		while not toil_state_obj and track_job_flag.is_set():
			try:
				root_job_id = root_job.jobStoreID
				if not job_store.check_if_job_exists(root_job_id):
					return
				if current_attempt != 0:
					job_store.setJobCache()
					toil_state(job_store,root_job)
				else:
					job_store_cache = job_store.job_cache
					toil_state_obj = toil_state(job_store,root_job,jobCache=job_store_cache)
			except:
				retry_message = "Jobstore not loaded properly, trying again ( Attempt " +str(current_attempt) + " / " + str(job_store_resume_attempts) + " )\n"+traceback.format_exc()
				log(logger,"info",retry_message)
				if current_attempt < job_store_resume_attempts:
					current_attempt = current_attempt + 1
					time.sleep(5)
				else:
					error_message = "Jobstore failed to load, check the workflow logs.\n"+traceback.format_exc()
					log(logger,"error",error_message)
					sys.exit(error_message)
		if not track_job_flag.is_set() or not toil_state_obj:
			return
		current_time = get_current_time()
		for single_job in job_store.getFailedJobs():
			job_name = single_job.jobName
			failed_job_log_file = single_job.logJobStoreFileID
			retry_count = single_job.remainingRetryCount
			jobstore_id = single_job.jobStoreID
			if retry_count == 0 and jobstore_id not in retry_job_ids:
				continue
			retry_count = retry_count + 1
			job_id = self.create_job_id(single_job.jobStoreID,retry_count)
			self.mark_job_as_failed(job_id,job_name)
		for single_job in job_store.getRestartedJobs():
			jobstore_id = single_job.jobStoreID
			retry_count = single_job.remainingRetryCount
			previous_retry_count = retry_count + 1
			retry_job_ids[jobstore_id] = retry_count
			job_name = single_job.jobName
			job_id = self.create_job_id(jobstore_id,previous_retry_count)
			self.mark_job_as_failed(job_id,job_name)
		for single_job, result in toil_state_obj.updatedJobs:
			job_name = single_job.jobName
			if job_name not in CWL_INTERNAL_JOBS:
				job_disk = single_job._disk/float(1e9)
				job_memory = single_job._memory/float(1e9)
				job_cores = single_job._cores
				jobstore_id = single_job.jobStoreID
				retry_count = single_job.remainingRetryCount
				if jobstore_id in retry_job_ids:
					retry_count = retry_job_ids[jobstore_id]
				job_id = self.create_job_id(jobstore_id,retry_count)
				job_stream = None
				job_info = None
				if single_job.command:
					job_stream = single_job.command.split(" ")[1]
				job_key = self.make_key_from_file(job_name,True)
				if job_stream:
					jobs_path[job_stream] = job_key
					job_stream_obj = self.read_job_stream(job_store_obj,job_stream)
					job_info = job_stream_obj['job_info']
				current_jobs.append(job_id)
				worker_obj = {"disk":job_disk,"memory":job_memory,"cores":job_cores,"job_stream":job_stream,"job_info":job_info}
				if job_key not in job_dict:
					job_dict[job_key] = {'submitted':{},'workers':{},'done':{},'exit':{}}
				tool_dict = job_dict[job_key]
				if job_id not in tool_dict['submitted']:
					tool_dict['submitted'][job_id] = current_time
				if job_id not in tool_dict['workers']:
					tool_dict['workers'][job_id] = worker_obj
		self.jobs_path = jobs_path
		self.current_jobs = current_jobs

	def make_key_from_file(self,job_name,use_basename):
		work_dir = self.work_dir
		workflow_id = 'toil-' + self.workflow_id
		if use_basename:
			job_id_with_extension = os.path.basename(job_name)
			job_id = os.path.splitext(job_id_with_extension)[0]
		else:
			job_id = os.path.relpath(job_name,work_dir)
			job_id.replace(workflow_id,"")
		safe_key = re.sub("["+punctuation+"]","_",job_id)
		return safe_key

	def read_job_stream(self,job_store_obj,job_stream_path):
		job_stream_file = job_store_obj.readFileStream(job_stream_path)
		job_stream_abs_path = job_store_obj._getAbsPath(job_stream_path)
		job_id = ""
		job_info = None
		if os.path.exists(job_stream_abs_path):
			with job_stream_file as job_stream:
				job_stream_contents = safeUnpickleFromStream(job_stream)
				job_stream_contents_dict = job_stream_contents.__dict__
				job_name = job_stream_contents_dict['jobName']
				if job_name not in CWL_INTERNAL_JOBS:
					job_id = self.make_key_from_file(job_name,True)
					if 'cwljob' in job_stream_contents_dict:
						job_info = job_stream_contents_dict['cwljob']
		return {"job_id":job_id,"job_info":job_info}

	def read_worker_log(self,worker_log_path):
		worker_jobs = self.worker_jobs
		logger = self.logger
		job_dict = self.jobs
		jobs_path = self.jobs_path
		read_only_job_store_obj = self.job_store_obj
		current_time = get_current_time()
		worker_log_key = self.make_key_from_file(worker_log_path,False)
		if os.path.isfile(worker_log_path):
			last_modified = self.get_file_modification_time(worker_log_path)
			if worker_log_key not in worker_jobs:
				worker_info = None
				worker_directory = os.path.dirname(worker_log_path)
				list_of_tools = []
				for root, dirs, files in os.walk(worker_directory):
					for single_file in files:
						if single_file == '.jobState':
							job_state = {}
							job_info = {}
							job_name = ''
							job_state_path = os.path.join(root,single_file)
							tool_key = None
							job_stream_path = ""
							with open(job_state_path,'rb') as job_state_file:
								job_state_contents = dill.load(job_state_file)
								job_state = job_state_contents
								job_stream_path = job_state_contents['jobName']
								tool_key = jobs_path[job_stream_path]
							if tool_key:
								if tool_key in job_dict:
									tool_dict = job_dict[tool_key]
									for single_job_key in tool_dict['workers']:
										single_worker_obj = tool_dict['workers'][single_job_key]
										if single_worker_obj["job_stream"] == job_stream_path:
											worker_info = {'job_state':job_state,'log_path':worker_log_path,'started':current_time,'last_modified': last_modified}
											single_worker_obj.update(worker_info)
											tool_info = (tool_key,single_job_key)
											list_of_tools.append(tool_info)
				worker_jobs[worker_log_key] = {'list_of_tools':list_of_tools}
			else:
				worker_jobs_tool_list = worker_jobs[worker_log_key]['list_of_tools']
				for single_tool,single_job_key in worker_jobs_tool_list:
					worker_info = job_dict[single_tool]['workers'][single_job_key]
					worker_info['last_modified'] = last_modified


	def check_for_running(self):
		work_dir = self.work_dir
		job_dict = self.jobs
		workflow_id = self.workflow_id
		for root, dirs, files in os.walk(work_dir):
			for single_file in files:
				if single_file == "worker_log.txt":
					worker_log_path = os.path.join(root,single_file)
					worker_info = self.read_worker_log(worker_log_path)

	def check_for_finished_jobs(self):
		job_dict = self.jobs
		current_jobs = self.current_jobs
		current_time = get_current_time()
		for single_tool_name in job_dict:
			single_tool = job_dict[single_tool_name]
			submitted_dict = single_tool['submitted']
			for single_job in submitted_dict:
				if single_job not in single_tool['done']:
					if single_job not in current_jobs:
						if single_job not in single_tool['exit']:
							single_tool['done'][single_job] = current_time

	def get_pending_and_running_jobs(self,submitted_dict,done_dict,exit_dict,workers_dict):
		logger = self.logger
		pending_dict = {}
		running_dict = {}
		current_time = get_current_time()
		for single_job in submitted_dict:
			if single_job not in done_dict and single_job not in exit_dict:
				if single_job in workers_dict:
					single_worker_obj = workers_dict[single_job]
					if 'started' not in single_worker_obj:
						pending_dict[single_job] = current_time
					else:
						started_time = single_worker_obj['started']
						last_modified = single_worker_obj['last_modified']
						running_obj = {'started':started_time,'last_modified':last_modified}
						running_dict[single_job] = running_obj
				else:
					error_message = str(single_job) + " not found in worker dictionary"
					log(logger,'error',error_message)

		return {'pending':pending_dict,'running':running_dict}


	def prepare_job_status(self):
		job_dict = self.jobs
		current_jobs = self.current_jobs
		job_status ={}
		for single_tool_name in job_dict:
			single_tool = job_dict[single_tool_name]
			submitted_dict = single_tool['submitted']
			workers_dict = single_tool['workers']
			exit_dict = single_tool['exit']
			done_dict = single_tool['done']
			pending_and_running_dict = self.get_pending_and_running_jobs(submitted_dict, done_dict, exit_dict, workers_dict)
			pending_dict = pending_and_running_dict['pending']
			running_dict = pending_and_running_dict['running']
			job_status[single_tool_name] = {'submitted':submitted_dict,'running':running_dict,'exit':exit_dict,'done':done_dict,'pending':pending_dict}
		return job_status

	def check_status(self, sleep_time, track_job_flag):
		logger = self.logger
		project_uuid = self.project_uuid
		job_status = None
		while track_job_flag.is_set():
			new_job_status = self.check_status_change(job_status,track_job_flag)
			if new_job_status:
				job_status = copy.deepcopy(new_job_status)
			time.sleep(sleep_time)

	def check_status_change(self,job_status,track_job_flag):
		logger = self.logger
		project_uuid = self.project_uuid
		self.check_jobs(track_job_flag)
		if not track_job_flag.is_set():
			return
		self.check_for_running()
		self.check_for_finished_jobs()
		new_job_status = self.prepare_job_status()
		status_change = self.get_change_status(job_status,new_job_status)
		jobs_dict = self.jobs
		if status_change:
			update_batch_system_run_results(logger,project_uuid,status_change,jobs_dict)
			self.print_change_status(status_change)
		return new_job_status

	def get_change_status(self,old_job_status, new_job_status):
		status_format = {'running':{'message':'{} is now running'},
					   'exit':{'message':'{} has exited'},
					   'done':{'message':'{} has finished'},
					   'pending':{'message':'{} is now pending'}}
		status_type = status_format.keys()
		status_name_dict = get_status_names()
		status_change = {}
		for single_tool in new_job_status:
			single_tool_obj = new_job_status[single_tool]
			for single_status in status_type:
				single_tool_status = single_tool_obj[single_status]
				if single_tool_status:
					for single_job_id in single_tool_status.keys():
						update_running_last_modified = False
						update_status = False
						if single_status == 'running':
							update_running_last_modied = True
						if not old_job_status or single_tool not in old_job_status or single_status not in old_job_status[single_tool] or single_job_id not in old_job_status[single_tool][single_status]:
							update_status = True
						if update_status or update_running_last_modified:
							status_template = status_format[single_status]['message']
							message = None
							if update_status:
								job_name = single_tool + '( ID: ' + single_job_id + ' )'
								message = status_template.format(job_name)
							status = status_name_dict[single_status]
							job_obj = {'single_tool_status':single_tool_status,'job_name':single_tool,'job_id':single_job_id,'status':status}
							status_change[single_job_id] = {'message':message,'job_obj':job_obj}
		return status_change

	def print_change_status(self,status_change):
		logger = self.logger
		for single_job in status_change:
			single_job_obj = status_change[single_job]
			single_job_message = single_job_obj['message']
			if single_job_message:
				log(logger,'info',single_job_message)

	def print_job_status(self,job_status):
		logger = self.logger
		if not job_status:
			return
		overview_status_list = []
		status_list = []
		status_dict = {'running':{'header':'Running:','status':'','total':0},
					   'exit':{'header':'Exit:','status':'','total':0},
					   'done':{'header':'Done:','status':'','total':0},
					   'pending':{'header':'Pending:','status':'','total':0}}
		status_items = status_dict.keys()
		log(logger,'info',job_status)
		for single_tool in job_status:
			single_tool_obj = job_status[single_tool]
			for single_status_key in status_dict:
				single_tool_status = single_tool_obj[single_status_key]
				single_status_dict = status_dict[single_status_key]
				tool_status_num = 0
				tool_status_str = single_status_dict['status']
				if single_status_key == 'running':
					if len(single_tool_status) != 0:
						tool_status_str = tool_status_str + "\t- " + str(single_tool) + "\n"
					for single_running_job_key in single_tool_status:
						single_running_job = single_tool_status[single_running_job_key]
						last_modified = single_running_job['last_modified']
						time_difference = get_time_difference(last_modified)
						time_difference_string = time_difference_to_string(time_difference,2)
						tool_status_str = tool_status_str + "\t\t- [ last modified: " + time_difference_string + " ago ]\n"
						tool_status_num = tool_status_num + 1
					if tool_status_num != 0:
						tool_status_str = tool_status_str + "\t\t- Total: " + str(tool_status_num) + "\n"
				else:
					tool_status_num = len(single_tool_status)
					if tool_status_num != 0:
						job_or_jobs_str = "jobs"
						if tool_status_num == 1:
							job_or_jobs_str = "job"
						tool_status_str = tool_status_str + "\t- " + str(single_tool) + " [ "+str(tool_status_num) + " " + job_or_jobs_str + " ]\n"
				single_status_dict['total'] = single_status_dict['total'] + tool_status_num
				single_status_dict['status'] = tool_status_str
		total_jobs = 0
		for single_status_key in status_dict:
			single_status_dict = status_dict[single_status_key]
			status_jobs = single_status_dict["total"]
			total_jobs = total_jobs + single_status_dict["total"]
			if status_jobs != 0:
				status_header = single_status_dict['header']
				status_str = status_header + "\n" + single_status_dict['status']
				status_jobs = status_header + " " + str(status_jobs)
				overview_status_list.append(status_jobs)
				status_list.append(status_str)
		overview_status = "Total Job(s): " + str(total_jobs) + " ( " + " ".join(overview_status_list) + " )"
		status = overview_status + "\n" +"\n".join(status_list)
		log(logger,'info',status)
