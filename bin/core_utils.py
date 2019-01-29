from __future__ import print_function
from subprocess import PIPE, Popen, STDOUT
import os, sys
import time
from shutil import copyfile
import filecmp
import logging
import json
import shlex
import signal
import socket
import getpass

starting_log_message="------------ starting ------------"
exiting_log_message="------------ exiting ------------"
finished_log_message="------------ finished ------------"

def run_popen(command,log_stdout,log_stderr,shell,wait,real_time):
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
        command_stdout = command_output['output'] + "\n----- log stdout -----\n" + output_log
        command_stderr = command_output['error'] +  "\n----- log stderr -----\n" + error_log
        print(command_stdout)
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

def check_user_kill_signal(project_name, project_uuid, pipeline_name, pipeline_version):
    log_dir, work_dir, tmp_path = get_dir_paths(project_name,project_uuid,pipeline_name,pipeline_version)
    from track_utils import termination_file_name
    user_log_path = os.path.join(log_dir,termination_file_name)
    if os.path.exists(user_log_path):
        with open(user_log_path) as user_log_file:
            user_log_data = json.load(user_log_file)
            return user_log_data
    else:
        return None

def save_json(json_path,json_data):
    with open(json_path,'w') as json_file:
        json.dump(json_data,json_file)

def send_user_kill_signal(project_name, project_uuid, pipeline_name, pipeline_version, termination_graceful):
    log_dir, work_dir, tmp_path = get_dir_paths(project_name,project_uuid,pipeline_name,pipeline_version)
    from track_utils import get_current_time, termination_file_name, submission_file_name
    current_user = getpass.getuser()
    current_hostname = socket.gethostname()
    user_termination_json = {'user':current_user,'hostname':current_hostname,'time':get_current_time(),'exit_graceful':termination_graceful,'error_message':None}
    user_log_path = os.path.join(log_dir,termination_file_name)
    user_submission_path = os.path.join(log_dir,submission_file_name)
    with open(user_submission_path) as user_submission_file:
        user_submission_data = json.load(user_submission_file)
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
    return merged_yaml

def create_roslin_yaml(output_meta_list, yaml_location, yaml_file_list):
    import ruamel.yaml as yaml

    result = None

    #yaml_data = []

    for single_output_meta_file in output_meta_list:
        if single_output_meta_file:
            with open(single_output_meta_file,'r') as output_meta_file:
                yaml_contents = yaml.safe_load(output_meta_file)
                #yaml_data.append(yaml_contents)
                result = merge(result,yaml_contents)

    for single_yaml_file in yaml_file_list:
        if single_yaml_file:
            with open(single_yaml_file,'r') as single_yaml:
                yaml_contents = yaml.safe_load(single_yaml)
                #yaml_data.append(yaml_contents)
                result = merge(result, yaml_contents)

    with open(yaml_location, 'w') as outfile:
        yaml.dump(result, outfile,  default_flow_style=False)

def convert_yaml_abs_path(inputs_yaml_path,base_dir,new_inputs_yaml_path):

    def convert_list(sample_list):
        #print(sample_list)
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


    import ruamel.yaml as yaml
    import json

    current_directory = os.getcwd()

    os.chdir(base_dir)

    with open(inputs_yaml_path,'r') as input_yaml_file:
        yaml_contents = yaml.safe_load(input_yaml_file)

    yaml_converted = convert_dict(yaml_contents)

    with open(new_inputs_yaml_path, 'w') as output_yaml_file:
        yaml.dump(yaml_converted, output_yaml_file,  default_flow_style=False)

    os.chdir(current_directory)

def check_if_env_is_empty(env_value):
    if env_value and env_value != 'None':
        return True
    else:
        return False

def copy_ignore_same_file(first_file,second_file):
    if os.path.exists(second_file):
        if filecmp.cmp(first_file,second_file):
            pass
    else:
        copyfile(first_file,second_file)

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