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

DOC_VERSION = "1.0.0"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z%z"


def bsub(bsubline):
    "execute lsf bsub"

    process = subprocess.Popen(bsubline, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = process.stdout.readline()

    # Expected output looks like: Job <26552430> is submitted to queue <controlR>.
    if re.match(r'Job <\d+> is submitted', output) is not None:
        lsf_job_id = int(output.strip().split()[1].strip('<>'))
    else:
        print("ERROR: Job submission failed", file=sys.stderr)
        sys.exit(1)

    return lsf_job_id


def submit_to_lsf(cmo_project_id, job_uuid, work_dir, pipeline_name_version, leader_node, workflow_name, restart_jobstore_uuid, debug_mode, single_node):
    "submit roslin-runner to the w node"

    batch_system = "lsf"
    node_request = ['-q', leader_node]
    # to use largeHG nodes, we don't have a queue, we have to request >376GB of RAM
    if leader_node == 'largeHG':
        node_request = ['-M', '512']
    # to submit short jobs, specify estimated run time as 59 minutes or less
    elif leader_node == 'short':
        node_request = ['-We', '0:59']

    # if a single-node was requested, use roslin-runner in singleMachine mode
    if single_node:
        batch_system = "singleMachine"

    lsf_proj_name = "{}:{}".format(cmo_project_id, job_uuid)
    job_name = "leader:{}:{}".format(cmo_project_id, job_uuid)
    job_desc = job_name
    output_dir = os.path.join(work_dir, "outputs")
    input_yaml = "inputs.yaml"

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

    bsubline = ["bsub"] + node_request + [
        "-P", lsf_proj_name,
        "-J", job_name,
        "-Jd", job_desc,
        "-cwd", work_dir,
        "-oo", "stdout.log",
        "-eo", "stderr.log",
        job_command
    ]

    lsf_job_id = bsub(bsubline)

    return lsf_proj_name, lsf_job_id


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


def publish_to_redis(cmo_project_id, cmo_project_path, lsf_proj_name, job_uuid):

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
        "--workflow",
        action="store",
        dest="workflow_name",
        help="CWL Workflow name (e.g. project-workflow.cwl)",
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

    parser.add_argument("--leader-node",
        action="store",
        dest="leader_node",
        choices=['controlR', 'control', 'largeHG', 'short'],
        default="controlR",
        help="The LSF node for the leader job. Default: controlR",
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

    params = parser.parse_args()

    # create a new unique job uuid
    job_uuid = str(uuid.uuid1())

    # read the Roslin Pipeline settings
    pipeline_settings = read_pipeline_settings(params.pipeline_name_version)

    # must be one of the singularity binding points
    work_base_dir = pipeline_settings["ROSLIN_PIPELINE_OUTPUT_PATH"]
    work_dir = os.path.join(work_base_dir, job_uuid[:8], job_uuid)

    # create only if work_dir does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    # copy input metadata files (mapping, grouping, paring, request, and inputs.yaml)
    input_metadata_filenames = [
        "inputs.yaml",
        "{}_request.txt".format(params.cmo_project_id),
        "{}_sample_grouping.txt".format(params.cmo_project_id),
        "{}_sample_mapping.txt".format(params.cmo_project_id),
        "{}_sample_pairing.txt".format(params.cmo_project_id)
    ]

    for filename in input_metadata_filenames:
        copyfile(
            os.path.join(params.cmo_project_path, filename),
            os.path.join(work_dir, filename)
        )
    clinical_data_file_name = "{}_sample_data_clinical.txt".format(params.cmo_project_id)
    if os.path.exists(os.path.join(params.cmo_project_path, clinical_data_file_name)):
        copyfile(
            os.path.join(params.cmo_project_path, clinical_data_file_name),
            os.path.join(work_dir, clinical_data_file_name)
        )


    # convert any relative path in inputs.yaml (e.g. path: ../abc)
    # to absolute path (e.g. path: /ifs/abc)
    convert_examples_to_use_abs_path(
        os.path.join(work_dir, "inputs.yaml")
    )

    # submit
    lsf_proj_name, lsf_job_id = submit_to_lsf(
        params.cmo_project_id,
        job_uuid,
        work_dir,
        params.pipeline_name_version,
        params.leader_node,
        params.workflow_name,
        params.restart_jobstore_uuid,
        params.debug_mode,
        params.single_node
    )

    print(lsf_proj_name)
    print(lsf_job_id)
    print(work_dir)

    # fixme: wait till leader job shows up
    time.sleep(5)

    publish_to_redis(params.cmo_project_id, params.cmo_project_path, lsf_proj_name, job_uuid)


if __name__ == "__main__":

    main()
