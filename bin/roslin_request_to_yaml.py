#!/usr/bin/env python3
import sys
import os
import re
import argparse
import copy
import csv
import glob
import uuid
import json
import subprocess
from collections import defaultdict
from core_utils import load_pipeline_settings

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
    command = [pipeline_script] + command_args
    proc = subprocess.Popen(command, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (script_stdout,script_stderr) = proc.communicate();
    if script_stdout:
        print(script_stdout.decode())
    if script_stderr:
        print(script_stderr.decode())

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
    pipeline_name_version_split = params.pipeline_name_version.split("/")
    pipeline_name = pipeline_name_version_split[0]
    pipeline_version = pipeline_name_version_split[1]
    pipeline_settings = load_pipeline_settings(pipeline_name,pipeline_version)
    run_command(params,pipeline_settings)
