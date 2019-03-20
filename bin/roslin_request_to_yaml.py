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

def read_pipeline_settings(pipeline_name_version):
    "read the Roslin Pipeline settings"
    settings_path = os.path.join(os.environ.get("ROSLIN_CORE_CONFIG_PATH"), pipeline_name_version, "settings.sh")
    command = ['bash', '-c', 'source {} && env'.format(settings_path)]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    source_env = {}
    for line in proc.stdout:
        (key, _, value) = line.partition("=")
        source_env[key] = value.rstrip()
    proc.communicate()
    return source_env

def run_command(params, pipeline_settings):
    script_name = 'roslin_request_to_yaml.py'
    pipeline_bin_path = pipeline_settings['ROSLIN_PIPELINE_BIN_PATH']
    pipeline_script = os.path.join(pipeline_bin_path,'scripts',script_name)
    params_dict = vars(params)
    command_args = []
    for single_arg_key in params_dict:
        single_arg = '--'+ single_arg_key
        single_arg = single_arg.replace('_','-')
        single_arg_value = params_dict[single_arg_key]
	if single_arg_value:
	    command_args.append(single_arg)
	    command_args.append(single_arg_value)
    command = ['python',pipeline_script] + command_args
    proc = subprocess.Popen(command, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (script_stdout,script_stderr) = proc.communicate();
    if script_stdout:
        print script_stdout
    if script_stderr:
        print script_stderr

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="convert current project files to yaml input")
    parser.add_argument("-m", "--mapping", help="the mapping file", required=True)
    parser.add_argument("-p", "--pairing", help="the pairing file", required=True)
    parser.add_argument("-g", "--grouping", help="the grouping file", required=True)
    parser.add_argument("-r", "--request", help="the request file", required=True)
    parser.add_argument("-f", "--yaml-output-file", help="file to write yaml to", required=True)
    parser.add_argument("--pipeline",action="store",dest="pipeline_name_version",help="Pipeline name/version (e.g. variant/2.5.0)",required=True)
    parser.add_argument("--clinical", help="the clinical data file", required=False)
    params = parser.parse_args()
    # read the Roslin Pipeline settings
    pipeline_settings = read_pipeline_settings(params.pipeline_name_version)
    run_command(params,pipeline_settings)
