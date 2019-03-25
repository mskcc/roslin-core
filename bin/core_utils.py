from __future__ import print_function
from builtins import super
from subprocess import PIPE, Popen, STDOUT
import os, sys
from multiprocessing.dummy import Pool
from queue import Queue
import time
import shutil
import filecmp
import logging
import json
import shlex
import signal
import socket
import glob
import getpass
import traceback

starting_log_message="------------ starting ------------"
exiting_log_message="------------ exiting ------------"
finished_log_message="------------ finished ------------"

def run_popen(command,log_stdout,log_stderr,shell,wait,real_time):
    try:
        pre_exec_fn = None
        if real_time:
            pre_exec_fn = os.setsid
            single_process = Popen(command, stdout=log_stdout,stderr=log_stderr, shell=shell, preexec_fn=pre_exec_fn)
        else:
            single_process = Popen(command, stdout=log_stdout,stderr=log_stderr, shell=shell)
        output = None
        error = None
        errorcode = None
        if real_time:
            output = ""
            error = ""
            subprocess_stdout = None
            subprocess_stderr = None
            if single_process.stdout:
                subprocess_stdout = iter(single_process.stdout.readline, "")
                for single_line in subprocess_stdout:
                    single_output_line = single_line.rstrip()
                    if single_output_line:
                        print(single_output_line)
                        output = output + "\n" + single_output_line
                        if exiting_log_message in single_output_line or finished_log_message in single_output_line:
                            single_process.stdout.close()
                            errorcode = single_process.wait()
                            return {"output":output,"error":error,"errorcode":errorcode}
                single_process.stdout.close()
                errorcode = single_process.wait()
        if wait:
            output, error = single_process.communicate()
            errorcode = single_process.returncode
        return {"output":output,"error":error,"errorcode":errorcode}
    except:
        error = traceback.format_exc()
        return {"output":None,"error":error,"errorcode":1}


def run_command(command,stdout_file,stderr_file,shell,wait):
    if stdout_file and stderr_file:
        with open(stdout_file,'w') as log_file_stdout, open(stderr_file,'w') as log_file_stderr:
            command_output = run_popen(command,log_file_stdout,log_file_stderr,shell,wait,False)
        output_log = ''
        error_log = ''
        if os.path.exists(stdout_file):
            with open(stdout_file,'r') as stdout_log:
                output_log = stdout_log.read()
        if os.path.exists(stderr_file):
            with open(stderr_file,'r') as stderr_log:
                error_log = stderr_log.read()
        command_stdout = ""
        command_stderr = ""
        if command_output['output']:
            command_stdout = command_output['output']
        if command_output['error']:
            command_stderr = command_output['error']
        if output_log:
            command_stdout = command_stdout + "\n----- log stdout -----\n" + output_log
        if error_log:
            command_stderr = command_stderr +  "\n----- log stderr -----\n" + error_log
        if command_stdout:
            print(command_stdout)
        if command_stderr:
            print_error(command_stderr)
        return command_output
    else:
        return run_popen(command,PIPE,PIPE,shell,wait,False)

def run_command_realtime(command,shell):
    return run_popen(command,PIPE,STDOUT,shell,False,True)

def print_error(*args, **kwargs):
    print(*args,file=sys.stderr, **kwargs)

def get_dir_paths(project_name, project_uuid,pipeline_name,pipeline_version):
    pipeline_settings = load_pipeline_settings(pipeline_name, pipeline_version)
    if not pipeline_settings:
        print_error("Error "+ str(pipeline_name) + " version "+ str(pipeline_version) + " does not exist.")
        exit(1)
    work_base_dir = pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"]
    work_dir = os.path.join(work_base_dir, project_uuid[:8], project_uuid)
    log_dir = os.path.join(work_dir,'log')
    tmp_path = os.path.join(pipeline_settings['ROSLIN_PIPELINE_BIN_PATH'],"tmp")
    return (log_dir,work_dir,tmp_path)

def get_jobstore_uuid(output_path):
    job_store_uuid_log = os.path.join(output_path,"job-store-uuid")
    if os.path.exists(job_store_uuid_log):
        with open(job_store_uuid_log) as job_store_uuid_obj:
            job_store_uuid = job_store_uuid_obj.readline().strip()
            return job_store_uuid
    else:
        return None

def get_pid(output_path):
    job_pid = os.path.join(output_path,"pid")
    if os.path.exists(job_pid):
        with open(job_pid) as job_pid_obj:
            job_pid = job_pid_obj.readline().strip()
            return job_pid
    else:
        return None

def kill_all_lsf_jobs(logger, uuid):
    if logger:
        from track_utils import log
    lsf_kill_str = "bjobs -P "+ str(uuid) + " -o \"jobid delimiter=','\" -noheader"
    lsf_kill_command = shlex.split(lsf_kill_command)
    kill_process_dict = run_command(lsf_kill_command,None,None,False,True)
    output = kill_process_dict['output']
    error = kill_process_dict['error']
    if "No unfinished job found"  not in error:
        if output:
            list_of_jobs_to_kill = output.split('\n')
            for single_job in list_of_jobs_to_kill:
                job_name_str = "bjobs -o \"job_name\"" + single_job + "-noheader"
                job_kill_str = "bkill " + single_job
                job_name_command = shlex.split(job_name_str)
                job_kill_command = shlex.split(job_kill_str)
                job_name_process = run_command(job_name_command,None,None,False,True)
                output = kill_process_dict['output']
                if output:
                    job_kill_message = "Killing LSF job ["+single_job+"] "+ output.rstrip()
                    if logger:
                        log(logger,'info',job_kill_message)
                    else:
                        print(job_kill_message)
                    job_kill_process = run_command(job_kill_command,None,None,False,True)
                    output = job_kill_process['output']
                    error = job_kill_process['error']
                    exit_code = job_kill_process['errorcode']
                    if exit_code != 0:
                        error_message = "Process exited with code " + exit_code + "\n"
                        if output:
                            error_message = "Output: " + output
                        if error:
                            error_message = "Error: " + error
                        if logger:
                            log(logger,'error',error_message)
                        else:
                            print_error(error_message)

def kill_project(project_name, project_uuid, work_dir, batch_system, user_termination_dict,tmp_path,termination_graceful):
    from track_utils import  update_run_results_status, get_status_names, add_user_event
    current_user = getpass.getuser()
    status_names = get_status_names()
    project_pid = get_pid(work_dir)
    if project_pid:
        if termination_graceful:
            try:
                os.kill(int(project_pid), signal.SIGINT)
            except OSError:
                print_error("Could not find pid "+str(project_pid)+ " . Project might have already exited")
        else:
            try:
                os.kill(int(project_pid), signal.SIGKILL)
            except OSError:
                print_error("Could not find pid "+str(project_pid)+ " . Project might have already exited")
            if batch_system == "LSF":
                kill_all_lsf_jobs(None,project_uuid)
            exit_status = status_names['exit']
            update_run_results_status(None,project_uuid,exit_status)
            project_killed_event = {"killed_by": "user"}
            project_killed_event.update(user_termination_dict)
            add_user_event(None,project_uuid,project_killed_event,"killed")

def kill_jobstore(job_store_path, termination_graceful):
    if termination_graceful:
        pid_path = os.path.join(job_store_path,'pid.log')
        print(pid_path)
        if os.path.exists(pid_path):
            with open(pid_path) as pid_file:
                pid_to_kill = pid_file.read().strip()
                print(pid_to_kill)
                os.kill(int(pid_to_kill), signal.SIGTERM)
    else:
        toil_kill_command = "toilKill " + str(job_store_path)
        toil_kill_command_split = shlex.split(toil_kill_command)
        run_command_realtime(toil_kill_command_split,False)

def check_user_kill_signal(project_name, project_uuid, pipeline_name, pipeline_version, dir_paths=None):
    if not dir_paths:
        log_dir, work_dir, tmp_path = get_dir_paths(project_name,project_uuid,pipeline_name,pipeline_version)
    else:
        (log_dir, work_dir, tmp_path) = dir_paths
    from track_utils import termination_file_name
    user_log_path = os.path.join(log_dir,termination_file_name)
    if os.path.exists(user_log_path):
        user_log_data = load_json(user_log_path)
        return user_log_data
    else:
        return None

def save_json(json_path,json_data):
    with open(json_path,'w') as json_file:
        json.dump(json_data,json_file)

def load_json(json_path):
    json_data = None
    with open(json_path) as json_file:
        json_data = json.load(json_file)
    return json_data

def load_yaml(yaml_path):
    import yaml
    yaml_data = None
    with open(yaml_path,'r') as yaml_file:
        yaml_data = yaml.load(yaml_file)
    return yaml_data

def check_yaml_boolean_value(yaml_value):
    yaml_true_value = ["y","Y","yes","Yes","YES","true","True","TRUE","on","On","ON"]
    yaml_false_value = ["n","N","no","No","NO","false","False","FALSE","off","Off","OFF"]
    if yaml_value in yaml_true_value:
        return True
    elif yaml_value in yaml_false_value:
        return False
    else:
        return None

def save_yaml(yaml_path,yaml_data):
    import yaml
    with open(yaml_path, 'w') as yaml_file:
        yaml.dump(yaml_data, yaml_file)

def convert_to_snake_case(input_str):
    first_str = re.sub('(.)[A-Z][a-z]+)',r'\1_\2',name)
    second_str = re.sub('([a-z0-9])([A-Z])',r'\1_\2',first_str).lower()
    return second_str


def send_user_kill_signal(project_name, project_uuid, pipeline_name, pipeline_version, termination_graceful):
    log_dir, work_dir, tmp_path = get_dir_paths(project_name,project_uuid,pipeline_name,pipeline_version)
    from track_utils import get_current_time, termination_file_name, submission_file_name
    current_user = getpass.getuser()
    current_hostname = socket.gethostname()
    user_termination_json = {'user':current_user,'hostname':current_hostname,'time':get_current_time(),'exit_graceful':termination_graceful,'error_message':None}
    user_log_path = os.path.join(log_dir,termination_file_name)
    user_submission_path = os.path.join(log_dir,submission_file_name)
    user_submission_data = load_json(user_submission_path)
    error_message = ""
    submitted_user = user_submission_data['user']
    submitted_hostname = user_submission_data['hostname']
    batch_system = user_submission_data["batch_system"]
    if current_user != submitted_user:
        error_message = str(current_user) + " cannot kill a job submitted by " + str(submitted_user)
    if current_hostname != submitted_hostname:
        error_message = "Cannot kill a job running on " + str(submitted_hostname) + " from " + str(current_hostname)
    if error_message:
        user_termination_json['error_message'] = error_message
        save_json(user_log_path,user_termination_json)
        exit_type = "gracefully"
        error_message = error_message + "\n" + " Sent a message to the leader job to exit " + exit_type
        print_error(error_message)
        exit(1)
    else:
        save_json(user_log_path,user_termination_json)
        kill_project(project_name,project_uuid,work_dir,batch_system,user_termination_json,tmp_path,termination_graceful)

def merge(yaml1, yaml2):
    if not yaml1 and yaml2:
        return yaml2

    if not yaml2 and yaml1:
        return yaml1

    merged_yaml = yaml1

    if isinstance(yaml1,dict) and isinstance(yaml2,dict):
        for k,v in yaml2.iteritems():
            if k not in merged_yaml:
                merged_yaml[k] = v
            else:
                merged_yaml[k] = merge(merged_yaml[k],v)
    if isinstance(yaml1,list) and isinstance(yaml2,list):
        merged_list = yaml1
        for single_elem in yaml2:
            if single_elem not in merged_list:
                merged_list.append(single_elem)
        merged_yaml = merged_list

    return merged_yaml

def create_roslin_yaml(output_meta_list, yaml_file_list):

    result = None
    cwd = os.getcwd()

    #yaml_data = []

    input_file_list = output_meta_list + yaml_file_list

    for input_file in input_file_list:
        if input_file:
            yaml_contents = yaml.load(input_file)
            file_location = os.path.dirname(input_file)
            os.chdir(file_location)
            yaml_converted = convert_dict(yaml_contents)
            os.chdir(cwd)
            result = merge(result, yaml_converted)
    return result

def convert_list(sample_list):
    if not sample_list:
        return sample_list
    new_list = []
    for single_item in sample_list:
        if isinstance(single_item,dict):
            new_item = convert_dict(single_item)
            new_list.append(new_item)
        elif isinstance(single_item,list):
            new_item = convert_list(single_item)
            new_list.append(new_item)
        else:
            new_list.append(single_item)
    return new_list

def convert_dict(sample_dict):
    new_dict = {}
    location_path = ""
    file_prefix = False
    file_key = ''
    new_dict = {}
    if not sample_dict:
        return sample_dict
    for single_key in sample_dict.keys():
        sample_obj = sample_dict[single_key]
        if isinstance(sample_obj,dict):
            new_obj = convert_dict(sample_obj)
        elif isinstance(sample_obj,list):
            sample_obj = convert_list(sample_obj)
        new_dict[single_key] = sample_obj
    if 'class' in new_dict:
        if new_dict['class'] == 'File':
            #print(new_dict)
            if 'location' in new_dict:
                file_key = 'location'
            elif 'path' in new_dict:
                file_key = 'path'
            if file_key:
                location_path = new_dict[file_key]
                if 'file://' in location_path:
                    file_prefix = True
                    location_path = location_path[7:]
                abs_path = os.path.abspath(location_path)
                new_location_path = ''
                if file_prefix:
                    new_location_path = 'file://' + abs_path
                else:
                    new_location_path = abs_path
                new_dict[file_key] = new_location_path
    return new_dict

def convert_yaml_abs_path(inputs_yaml_path,base_dir,new_inputs_yaml_path):

    current_directory = os.getcwd()

    os.chdir(base_dir)

    yaml_contents = load_yaml(inputs_yaml_path)

    yaml_converted = convert_dict(yaml_contents)

    save_yaml(new_inputs_yaml_path, yaml_converted)

    os.chdir(current_directory)

def check_if_env_is_empty(env_value):
    if env_value and env_value != 'None':
        return False
    else:
        return True

def copy_ignore_same_file(first_file,second_file):
    if os.path.exists(second_file):
        if filecmp.cmp(first_file,second_file):
            return
    shutil.copyfile(first_file,second_file)

def read_pipeline_settings(pipeline_name, pipeline_version):
    "read the Roslin Pipeline settings"

    pipeline_name_version = os.path.join(pipeline_name,pipeline_version)
    settings_path = os.path.join(os.environ.get("ROSLIN_CORE_CONFIG_PATH"), pipeline_name_version, "settings.sh")

    if not os.path.exists(settings_path):
        return None

    command = ['bash', '-c', 'source {} && env'.format(settings_path)]

    proc = Popen(command, stdout=PIPE)

    source_env = {}

    for line in proc.stdout:
        (key, _, value) = line.partition("=")
        source_env[key] = value.rstrip()

    proc.communicate()

    return source_env

def load_pipeline_settings(pipeline_name, pipeline_version):
    pipeline_settings = read_pipeline_settings(pipeline_name, pipeline_version)
    if not pipeline_settings:
        return None
    roslin_pipeline_data_path = pipeline_settings['ROSLIN_PIPELINE_DATA_PATH']
    roslin_virtualenv_path = os.path.join(roslin_pipeline_data_path,"virtualenv","bin","activate_this.py")
    execfile(roslin_virtualenv_path, dict(__file__=roslin_virtualenv_path))
    return pipeline_settings

def chunks(l, n):
    "split a list into a n-size chunk"
    l.sort()
    # for item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # create an index range for l of n items:
        yield l[i:i + n]


def create_file_list(src_dir, glob_patterns):
    "create a list object that contains all the files to be copied"

    file_list = list()

    # iterate through glob_patterns
    # construct a list that contains all the files to be copied
    for glob_pattern in glob_patterns:
        file_list.extend(glob.glob(os.path.join(src_dir, glob_pattern)))

    return list(set(file_list))


def create_parallel_cp_commands(file_list, dst_dir, num_workers, worker_threads, worker_num, worker_queue):
    "create a parallel cp command"



    cmds = list()
    groups = list()

    groups = list(chunks(file_list, num_workers))

    worker_group = None
    cmd = None
    group_length = 0

    if worker_num < len(groups):
        worker_group = groups[worker_num]

    if worker_group:
        group_length = len(worker_group)
        #tempfile.tempdir=tmp_path
        #with tempfile.NamedTemporaryFile(delete=False) as file_temp:
        for filename in worker_group:
            #file_temp.write(filename + "\n")
            cmd = '/bin/cp {} {}'.format(filename,dst_dir)
            cmd_obj = {'command':cmd,'queue':worker_queue}
            cmds.append(cmd_obj)
            #cmd = 'parallel -a ' + file_temp.name + ' -j+' + str(worker_threads) + ' yes | cp {} ' + dst_dir

    return cmds

def check_if_argument_file_exists(argument_value):
    if argument_value:
        if not os.path.exists(argument_value):
            print_error("ERROR: Could not find " + argument_value)
            sys.exit(1)

def copy_worker(copy_command_dict):
    copy_command = copy_command_dict['command']
    copy_queue = copy_command_dict['queue']
    copy_process = run_command(shlex.split(copy_command),None,None,False,True)
    copy_process['command'] = copy_command
    copy_queue.put(copy_process)

def copy_outputs(params,job_params):
    "copy output files in toil work dir to the final destination"
    from track_utils import log, find_unique_name_in_dir, old_jobs_folder, add_file_handler, ROSLIN_COPY_OUTPUTS_LOG
    work_dir = params['project_work_dir']
    tmp_dir = params['work_dir']
    log_path = params['log_folder']
    debug_mode = params['debug_mode']
    output_config = params['copy_outputs_config']
    out_dir = params['results_dir']
    folder_key = job_params['folder_key']
    max_workers = job_params['max_workers']
    worker_num = job_params['worker_num']
    worker_threads = job_params['worker_threads']
    job_name = job_params['name']
    tmp_folder = job_name
    tmp_path = os.path.join(tmp_dir,tmp_folder)
    log_folder = log_path
    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)
    logger = logging.getLogger(job_name)
    log_file_path = os.path.join(log_folder,ROSLIN_COPY_OUTPUTS_LOG)
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        add_file_handler(logger,log_file_path,None,logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        add_file_handler(logger,log_file_path,None,logging.INFO)
    folder_data = output_config[folder_key]
    cmd_list = []
    worker_queue = Queue()
    worker_pool = Pool(worker_threads)
    for single_folder_elem in folder_data:
        file_patterns = single_folder_elem["patterns"]
        dst_base_dir = os.path.join(out_dir,folder_key)
        if "input_folder" in single_folder_elem:
            src_folder = single_folder_elem["input_folder"]
            src_dir = os.path.join(work_dir,src_folder)
        else:
            src_dir = work_dir
        if "output_folder" in single_folder_elem:
            dst_folder = single_folder_elem["output_folder"]
            dst_dir = os.path.join(dst_base_dir,dst_folder)
        else:
            dst_dir = dst_base_dir
        if not os.path.isdir(dst_dir):
            os.makedirs(dst_dir)
        file_list = create_file_list(src_dir,file_patterns)
        single_folder_cmd_list = create_parallel_cp_commands(file_list,dst_dir,max_workers,worker_threads,worker_num,worker_queue)
        cmd_list.extend(single_folder_cmd_list)
    group_length = len(cmd_list)
    copy_info = "[ {} threads: {} ] Copying {} file(s) from {} to {}\n".format(str(job_name),str(worker_threads),str(group_length),src_dir,dst_dir)
    log(logger,"info",copy_info)
    if cmd_list:
        copy_worker(cmd_list[0])
        try:
            copy_results = worker_pool.map(copy_worker,cmd_list)
        except:
            pass
        worker_queue.put(None)
        for single_output in iter(worker_queue.get, None):
            single_output_errorcode = single_output['errorcode']
            single_output_command = single_output['command']
            single_output_str = ""
            if single_output['output']:
                single_output_str = single_output['output']
            if single_output['error']:
                single_output_str = single_output['error']
            single_output_id = "[ {} threads: {} ] Command:\n {}\n".format(str(job_name),str(worker_threads),str(single_output_command))
            if single_output_errorcode != 0:
                single_output_message = single_output_id + "Failed\n {}".format(str(single_output_str))
                log(logger,"error",single_output_message)
                return single_output_errorcode
            else:
                if debug_mode:
                    single_output_message = single_output_id + "Finished\n {}".format(str(single_output_str))
                    log(logger,"debug",single_output_message)
        worker_pool.close()
        worker_pool.join()
    return 0