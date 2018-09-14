#!/usr/bin/env python

from __future__ import print_function
from shutil import copyfile
import os, sys, glob, uuid, argparse, subprocess
import hashlib
import datetime
import json
import tarfile
import base64
import time
import re
import pytz
import redis
import subprocess
import shlex
import yaml
import json2yaml


DOC_VERSION = "1.0.0"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"

def build_workflow_command(cmo_project_id, job_uuid, pipeline_name_version, batch_system, restart_jobstore_uuid, debug_mode, single_node, input_yaml, output_dir, workflow_name):

    if pipeline_name_version != None:
        job_command = "roslin-runner.sh -v {} -w {} -i {} -b {} -p {} -j {} -o {}".format(
            pipeline_name_version,
            workflow_name,
            input_yaml,
            batch_system,
            cmo_project_id,
            job_uuid,
            output_dir
        )
    else:
        job_command = "roslin-runner.sh -w {} -i {} -b {} -p {} -j {} -o {}".format(
            workflow_name,
            input_yaml,
            batch_system,
            cmo_project_id,
            job_uuid,
            output_dir
        )

    # add "-r" if restart jobstore uuid is supplied
    if restart_jobstore_uuid:
        job_command = job_command + " -r {}".format(restart_jobstore_uuid)

    # add "-d" if debug_mode is turned on
    if debug_mode:
        job_command = job_command + " -d"

    return job_command

def run_workflow(params, input_yaml, workflow_name, output_dir_name):

    lsf_proj_name = "{}:{}".format(params.cmo_project_id, params.job_uuid)
    job_name = "leader:{}:{}".format(params.cmo_project_id, params.job_uuid)
    job_desc = job_name
    output_dir = os.path.join(params.work_dir, output_dir_name)
    output_meta_json = output_dir + os.sep + 'output-meta.json'

    job_command = build_workflow_command(
        params.cmo_project_id,
        params.job_uuid,
        params.pipeline_name_version,
        params.batch_system,
        params.restart_jobstore_uuid,
        params.debug_mode,
        params.single_node,
        input_yaml,
        output_dir,
        workflow_name
    )

    # add check if this workflow was previously completed; if complete, return 0

    args = shlex.split(job_command)

    print(job_command)
    process_run = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return process_run, output_meta_json

# fixme: move to common
def get_current_utc_datetime():
    "return the current UTC date/time"

    utc_dt = datetime.datetime.utcnow()

    utc_dt = pytz.timezone("UTC").localize(utc_dt, is_dst=None)

    return utc_dt.strftime(DATETIME_FORMAT)


def generate_sha1(filename):

    # 64kb chunks
    buf_size = 65536

    sha1 = hashlib.sha1()

    with open(filename, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()


def targzip_project_files(cmo_project_id, cmo_project_path):

    files = glob.glob(os.path.join(cmo_project_path, "*"))

    tgz_path = "{}.tgz".format(os.path.join(cmo_project_path, cmo_project_id))
    tar = tarfile.open(tgz_path, mode="w:gz", dereference=True)
    for filename in files:
        tar.add(filename)
    tar.close()

    with open(tgz_path, "rb") as tgz_file:
        return base64.b64encode(tgz_file.read())


def convert_examples_to_use_abs_path(inputs_yaml_path):
    "convert example inputs.yaml to use absolute path"

    output = []

    # fixme: best way is to look for:
    #   class: File
    #   path: ../abc/def/123.fastq
    with open(inputs_yaml_path, "r") as yaml_file:
        lines = yaml_file.readlines()
        prev_line = ""
        for line in lines:
            line = line.rstrip("\n")

            # if "class: File" in prev_line:
            #     # fixme: pre-compile
            #     # path: ../ or path: ./
            #     pattern = r"path: (\.\.?/.*)"
            #     match = re.search(pattern, line)
            #     if match:
            #         path = os.path.abspath(match.group(1))
            #         line = re.sub(pattern, "path: {}".format(path), line)

            # fixme: pre-compile
            # path: ../ or path: ./
            pattern = r"path: (\.\.?/.*)"
            match = re.search(pattern, line)
            if match:
                path = os.path.abspath(match.group(1))
                line = re.sub(pattern, "path: {}".format(path), line)

            output.append(line)
            prev_line = line

    with open(inputs_yaml_path, "w") as yaml_file:
        yaml_file.write("\n".join(output))


def construct_project_metadata(cmo_project_id, cmo_project_path, job_uuid):

    request_file = os.path.abspath(
        os.path.join(cmo_project_path, cmo_project_id + "_request.txt")
    )
    mapping_file = os.path.abspath(
        os.path.join(cmo_project_path, cmo_project_id + "_sample_mapping.txt")
    )
    grouping_file = os.path.abspath(
        os.path.join(cmo_project_path, cmo_project_id + "_sample_grouping.txt")
    )
    pairing_file = os.path.abspath(
        os.path.join(cmo_project_path, cmo_project_id + "_sample_pairing.txt")
    )

    # skip if any of this file is missing
    if not os.path.isfile(request_file) or not os.path.isfile(mapping_file) \
            or not os.path.isfile(grouping_file) or not os.path.isfile(pairing_file):
        return None

    tgz_blob = targzip_project_files(cmo_project_id, cmo_project_path)

    project = {
        "version": DOC_VERSION,
        "projectId": cmo_project_id,
        "pipelineJobId": job_uuid,
        "dateSubmitted": get_current_utc_datetime(),
        "inputFiles": {
            "blob": tgz_blob,
            "request": {
                "path": request_file,
                "checksum": "sha1$" + generate_sha1(request_file)
            },
            "mapping": {
                "path": mapping_file,
                "checksum": "sha1$" + generate_sha1(mapping_file)
            },
            "grouping": {
                "path": grouping_file,
                "checksum": "sha1$" + generate_sha1(grouping_file)
            },
            "pairing": {
                "path": pairing_file,
                "checksum": "sha1$" + generate_sha1(pairing_file)
            }
        }
    }

    return project


def publish_to_redis(cmo_project_id, cmo_project_path, job_uuid):

    lsf_proj_name = "{}:{}".format(cmo_project_id, job_uuid)

    # fixme: wait until leader job shows up in LSF
    data = construct_project_metadata(cmo_project_id, cmo_project_path, job_uuid)

    if not data:
        return

    # connect to redis
    redis_host = os.environ.get("ROSLIN_REDIS_HOST")
    redis_port = int(os.environ.get("ROSLIN_REDIS_PORT"))
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

    redis_client.publish('roslin-projects', json.dumps(data))

# fixme: common.lib
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

def main():
    "main function"

    parser = argparse.ArgumentParser(description='submit')

    parser.add_argument(
        "--id",
        action="store",
        dest="cmo_project_id",
        help="CMO Project ID (e.g. Proj_5088_B)",
        required=True
    )

    parser.add_argument(
        "--path",
        action="store",
        dest="cmo_project_path",
        help="Path to CMO Project (e.g. /ifs/projects/CMO/Proj_5088_B)",
        required=True
    )

    parser.add_argument(
        "--pipeline",
        action="store",
        dest="pipeline_name_version",
        help="Pipeline name/version (e.g. variant/2.4.0)",
        required=True
    )

    parser.add_argument(
        "--restart",
        action="store",
        dest="restart_jobstore_uuid",
        help="jobstore uuid for restart",
        required=False
    )

    parser.add_argument("--batch-system",
        action="store",
        dest="batch_system",
        default="lsf",
        choices=["lsf", "singleMachine", "mezos"],
        help="Which version of roslin-runner to run (lsf, singleMachine, or mezos)",
        required=False)

    parser.add_argument(
        "--single-node",
        action="store_true",
        dest="single_node",
        help="Run the runner in singleMachine mode (Recommend setting --leader-node largeHG)"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        dest="debug_mode",
        help="Run the runner in debug mode"
    )

    parser.add_argument(
        "--input-yaml",
        action="store",
        dest="input_yaml_file",
        default="inputs.yaml",
        help="The input YAML file"
    )

    parser.add_argument(
        "--job-uuid",
        action="store",
        dest="job_uuid",
        default=None,
        help="Existing job UUID"
    )

    params = parser.parse_args()

    print(params)

    # create a new unique job uuid
    params.job_uuid = params.job_uuid if params.job_uuid is not None else str(uuid.uuid1())

    # read the Roslin Pipeline settings
    pipeline_settings = read_pipeline_settings(params.pipeline_name_version)

    # must be one of the singularity binding points
    work_base_dir = pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"]
    work_dir = os.path.join(work_base_dir, params.job_uuid[:8], params.job_uuid)
    params.work_dir = work_dir

    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    # copy input metadata files (mapping, grouping, paring, request, and inputs.yaml) if they don't already exist
    input_metadata_filenames = [
        "{}_request.txt".format(params.cmo_project_id),
        "{}_sample_grouping.txt".format(params.cmo_project_id),
        "{}_sample_mapping.txt".format(params.cmo_project_id),
        "{}_sample_pairing.txt".format(params.cmo_project_id)
    ]

    input_yaml_orig = params.input_yaml_file
    input_yaml_dest = os.path.join(work_dir, os.path.basename(params.input_yaml_file))
    if not os.path.exists(input_yaml_dest):
        copyfile( input_yaml_orig, input_yaml_dest )

    for filename in input_metadata_filenames:
        orig = os.path.join(params.cmo_project_path, filename)
        dest = os.path.join(work_dir, filename)
        if not os.path.exists(dest):
            copyfile( orig, dest )

    clinical_data_file_name = "{}_sample_data_clinical.txt".format(params.cmo_project_id)
    if os.path.exists(os.path.join(params.cmo_project_path, clinical_data_file_name)):
        orig = os.path.join(params.cmo_project_path, clinical_data_file_name)
        dest = os.path.join(work_dir, clinical_data_file_name)
        if not os.path.exists(dest):
            copyfile( orig, dest )

    # convert any relative path in inputs.yaml (e.g. path: ../abc)
    # to absolute path (e.g. path: /ifs/abc)
    convert_examples_to_use_abs_path( input_yaml_dest )

#    publish_to_redis(params.cmo_project_id, params.cmo_project_path, job_uuid) # create initial db entry for this project 

    # determine if we're doing find_svs by looking through request_file
    # TODO: Clean up
    tmp_yaml_data = yaml.load(open(input_yaml_dest, 'rb'))
    request_file_path = tmp_yaml_data['db_files']['request_file']['path']
    request_data = open(request_file_path, 'rb')
    find_sv = False
    for line in request_data:
        data = line.split(":")
        if len(data) > 1:
            if data[0] == "Assay":
                if "IMPACT" in data[1] or "HemePACT" in data[1]:
                    print("here")
                    find_sv = True

    # Prototyping this for now
    alignment_process, alignment_json = run_workflow(
        params,
        input_yaml_dest,
        "alignment.cwl",
        "alignment"
    )

    alignment_process.wait()
 
    if alignment_process.returncode == 1:
        print("Alignment broke; exiting")
        sys.exit(1)

    print("Alignment done.")

    post_alignment_path = os.path.join(params.work_dir, "post-alignment.yaml")
    post_alignment_yaml = json2yaml.create_roslin_yaml(
            alignment_json, 
            post_alignment_path,
            input_yaml_dest
    )

    gather_metrics_process, gather_metrics_json = run_workflow(
            params,
            post_alignment_yaml,
            "gather_metrics.cwl",
            "gather_metrics"
    )

    conpair_process, conpair_json = run_workflow(
            params,
            post_alignment_yaml,
            "conpair.cwl",
            "conpair"
    )

    running_processes = [ gather_metrics_process, conpair_process ]

    # add find_svs conditional
    if find_sv:
        find_svs_process, find_svs_json = run_workflow(
                params,
                post_alignment_yaml,
                "find_svs.cwl",
                "find_svs"
        )
        
        running_processes.append(find_svs_process)

    variant_calling_process, variant_calling_json = run_workflow(
            params,
            post_alignment_yaml,
            "variant_calling.cwl",
            "variant_calling"
    )

    variant_calling_process.wait()
    
    if variant_calling_process.returncode == 1:
        print("Variant Calling broke; can't do filtering. Check.")
        sys.exit(1)

    post_variant_calling_path = os.path.join(params.work_dir, "post-variant-calling.yaml")
    post_variant_calling_yaml = json2yaml.create_roslin_yaml(
            variant_calling_json, 
            post_variant_calling_path,
            post_alignment_yaml
    )       

    filtering_process, filtering_json = run_workflow(
            params,
            post_variant_calling_yaml,
            "filtering.cwl",
            "filtering"
    )

    running_processes.append(filtering_process)
    
    # TODO: Add checks for successful processes
    num_processes = len(running_processes)
    failed_processes = set()
    successful_processes = set()
    while len(failed_processes) + len(successful_processes) < num_processes:
        for running_process in running_processes:
            returncode = running_process.poll()
            if returncode == 1:
                failed_processes.add(running_process)
            if returncode == 0:
                successful_processes.add(running_process)
            time.sleep(.5)
        time.sleep(5)

    num_success = len(successful_processes)
    num_fail = len(failed_processes)

    print("Summary, not including alignment process")
    print("Num success %i" % num_success)
    print("Num fail %i" % num_fail)

if __name__ == "__main__":
    main()
