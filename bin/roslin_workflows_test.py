#!/usr/bin/env python
from __future__ import print_function
import pytest
import os
import sys
import json
import atexit
from shutil import copyfile
import getpass
ROSLIN_CORE_BIN_PATH = os.environ['ROSLIN_CORE_BIN_PATH']
sys.path.append(ROSLIN_CORE_BIN_PATH)
from core_utils  import run_command, print_error, send_user_kill_signal, get_choices
test_dir = ''
if 'LOG_TEST' in os.environ:
    test_dir = os.environ['LOG_TEST']
else:
    test_dir = os.getcwd()
test_list = []

def get_examples_path():
    examples_path = os.environ['ROSLIN_EXAMPLE_PATH']
    return examples_path

def cleanup():
    for single_folder in test_list:
        submission_data = {}
        user_examples_path = get_examples_path()
        test_path = os.path.join(user_examples_path,single_folder)
        submission_file_path = os.path.join(test_path,"submission.json")
        if os.path.exists(submission_file_path):
            with open(submission_file_path,"r") as submission_file:
                submission_data = json.load(submission_file)
            project_name = submission_data['project_id']
            project_uuid = submission_data['job_uuid']
            pipeline_name = submission_data['pipeline_name']
            pipeline_version = submission_data['pipeline_version']
            send_user_kill_signal(project_name,project_uuid,pipeline_name,pipeline_version,True)

def move_logs(folder_name,log_folder):
    for root, dirs, files in os.walk(log_folder):
        file_prefix = ''
        if root != log_folder:
            file_prefix = os.path.basename(root)
        for single_file in files:
            if single_file.endswith(".log"):
                file_basename = os.path.basename(single_file)
                file_basename_no_ext = os.path.splitext(file_basename)[0]
                new_file_name = str(folder_name) + "_" + str(file_prefix) + file_basename_no_ext + ".txt"
                new_file_path = os.path.join(test_dir,new_file_name)
                single_file_path = os.path.join(root,single_file)
                copyfile(single_file_path,new_file_path)

def run_test(folder_name):
    global test_list
    test_list.append(folder_name)
    submission_data = {}
    user_examples_path = get_examples_path()
    test_path = os.path.join(user_examples_path,folder_name)
    os.chdir(test_path)
    run_example_command = ['./run-example.sh']
    stdout_file = str(folder_name) + '_leader_stdout.txt'
    stderr_file = str(folder_name) + '_leader_stderr.txt'
    stdout_file_path = os.path.join(test_path,stdout_file)
    stderr_file_path = os.path.join(test_path,stderr_file)
    output = run_command(run_example_command,stdout_file_path,stderr_file_path,False,True)
    submission_file_path = os.path.join(test_path,"submission.json")
    if os.path.exists(submission_file_path):
        with open(submission_file_path,"r") as submission_file:
            submission_data = json.load(submission_file)
        log_folder = submission_data['log_dir']
        new_stdout_file_path = os.path.join(test_dir,stdout_file)
        new_stderr_file_path = os.path.join(test_dir,stderr_file)
        if os.path.exists(stdout_file_path):
            copyfile(stdout_file_path,new_stdout_file_path)
        if os.path.exists(stderr_file_path):
            copyfile(stderr_file_path,new_stderr_file_path)
        move_logs(folder_name,log_folder)
    if not output:
        cleanup()
    elif output['errorcode'] != 0:
        cleanup()
    assert output
    assert output['errorcode'] == 0

@pytest.mark.parametrize("workflow", get_choices()['workflow_name'])
def test_workflow(workflow):
    run_test(workflow)
