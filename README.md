# Roslin-core

> Roslin-core handles submission, tracking, killing and restarting of projects through a roslin pipeline

## Usage Guide

## Tracking

All projects are automatically tracked on the mongo database specified in the core config. You can disable this by setting the ```ROSLIN_MONGO_DISABLE``` environment variable

```
export ROSLIN_MONGO_DISABLE="True"
```

### Submit a project

Use the script `roslin_submit.py` to submit projects

#### Arguments

```
usage: roslin_submit.py [-h] --name {variant} --version {2.5.0} --id
                        PROJECT_ID --inputs INPUTS_YAML --workflow
                        {Alignment,CdnaContam,GatherMetrics,GenerateQc,GenerateQcSV,MafProcessing,PairWorkflow,PairWorkflowSV,Realignment,SampleWorkflow,StructuralVariants,VariantCalling,VariantWorkflow,VariantWorkflowSV}
                        --batch-system
                        {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}
                        [--cwl-batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}]
                        [--debug] [--path PROJECT_PATH] [--test-mode]
                        [--force-overwrite-results] [--on-start ON_START]
                        [--on-complete ON_COMPLETE] [--on-fail ON_FAIL]
                        [--on-success ON_SUCCESS] [--use-docker]
                        [--docker-registry DOCKER_REGISTRY]
                        [--foreground-mode] [--results RESULTS_DIR]
                        [--max-mem MAX_MEM] [--max-cpu MAX_CPU]

optional arguments:
  -h, --help            show this help message and exit
  --name {variant}      Pipeline name (default: None)
  --version {2.5.0}     Pipeline versions: (default: None)
  --id PROJECT_ID       Project ID (e.g. Proj_5088_B) (default: None)
  --inputs INPUTS_YAML  The path to your input yaml file ( required on non-
                        restart runs ) (default: None)
  --workflow {Alignment,CdnaContam,GatherMetrics,GenerateQc,GenerateQcSV,MafProcessing,PairWorkflow,PairWorkflowSV,Realignment,SampleWorkflow,StructuralVariants,VariantCalling,VariantWorkflow,VariantWorkflowSV}
                        Workflow name ( required on non-restart runs )
                        (default: None)
  --batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}
                        The batch system to submit the job (default: None)
  --cwl-batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}
                        The batch system to submit the cwl jobs (uses --batch-
                        system if not set) (default: None)
  --debug               Run the runner in debug mode (default: False)
  --path PROJECT_PATH   Path to Project files (to store in database) (default:
                        None)
  --test-mode           Run the runner in test mode (default: False)
  --force-overwrite-results
                        Force overwrite if results folder already exists
                        (default: False)
  --on-start ON_START   Python script to run when the workflow starts
                        (default: None)
  --on-complete ON_COMPLETE
                        Python script to run when the workflow completes
                        (either fail or succeed) (default: None)
  --on-fail ON_FAIL     Python script to run when the workflow fails (default:
                        None)
  --on-success ON_SUCCESS
                        Python script to run when the workflow succeeds
                        (default: None)
  --use-docker          Use Docker instead of singularity (default: False)
  --docker-registry DOCKER_REGISTRY
                        Dockerhub registry to pull ( invoked only with --use-
                        docker) (default: None)
  --foreground-mode     Runs the pipeline the the foreground (default: False)
  --results RESULTS_DIR
                        Path to the directory to store results (default: None)
  --max-mem MAX_MEM     The maximum amount of memory to request in GB (e.g.
                        8G) (default: None)
  --max-cpu MAX_CPU     The maximum amount of cpu to request (default: None)
```

### Kill a project

Use the script `roslin_kill_project.py` to kill projects

#### Arguments

```
usage: roslin_kill_project.py [-h] --name PIPELINE_NAME --version
                              PIPELINE_VERSION --id PROJECT_ID --uuid
                              PROJECT_UUID [--forceful]

roslin kill project

optional arguments:
  -h, --help            show this help message and exit
  --name PIPELINE_NAME  Pipeline name (e.g. variant)
  --version PIPELINE_VERSION
                        Pipeline version (e.g. 2.4.0)
  --id PROJECT_ID       Project ID (e.g. Proj_5088_B)
  --uuid PROJECT_UUID   The uuid of the project
  --forceful            Forcefully kill the project
 ```

 ### Restart a project

 Use the script `roslin_restart.py` to restart a project

 #### Arguments
 ```
 usage: roslin_restart.py [-h] --name {variant} --version {2.5.0} --restart
                         RESTART_JOB_UUID [--id PROJECT_ID]
                         [--batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}]
                         [--cwl-batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}]
                         [--debug] [--path PROJECT_PATH] [--test-mode]
                         [--force-overwrite-results] [--on-start ON_START]
                         [--on-complete ON_COMPLETE] [--on-fail ON_FAIL]
                         [--on-success ON_SUCCESS] [--use-docker]
                         [--docker-registry DOCKER_REGISTRY]
                         [--foreground-mode] [--results RESULTS_DIR]
                         [--max-mem MAX_MEM] [--max-cpu MAX_CPU]

optional arguments:
  -h, --help            show this help message and exit
  --name {variant}      Pipeline name (default: None)
  --version {2.5.0}     Pipeline versions: (default: None)
  --restart RESTART_JOB_UUID
                        project uuid for restart (default: None)
  --id PROJECT_ID       Project ID (e.g. Proj_5088_B) (default: None)
  --batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}
                        The batch system to submit the job (default: None)
  --cwl-batch-system {LSF,Mesos,Torque,Slurm,HTCondor,singleMachine,parasol,gridEngine}
                        The batch system to submit the cwl jobs (uses --batch-
                        system if not set) (default: None)
  --debug               Run the runner in debug mode (default: False)
  --path PROJECT_PATH   Path to Project files (to store in database) (default:
                        None)
  --test-mode           Run the runner in test mode (default: False)
  --force-overwrite-results
                        Force overwrite if results folder already exists
                        (default: False)
  --on-start ON_START   Python script to run when the workflow starts
                        (default: None)
  --on-complete ON_COMPLETE
                        Python script to run when the workflow completes
                        (either fail or succeed) (default: None)
  --on-fail ON_FAIL     Python script to run when the workflow fails (default:
                        None)
  --on-success ON_SUCCESS
                        Python script to run when the workflow succeeds
                        (default: None)
  --use-docker          Use Docker instead of singularity (default: False)
  --docker-registry DOCKER_REGISTRY
                        Dockerhub registry to pull ( invoked only with --use-
                        docker) (default: None)
  --foreground-mode     Runs the pipeline the the foreground (default: False)
  --results RESULTS_DIR
                        Path to the directory to store results (default: None)
  --max-mem MAX_MEM     The maximum amount of memory to request in GB (e.g.
                        8G) (default: None)
  --max-cpu MAX_CPU     The maximum amount of cpu to request (default: None)
  ```

Note: restart may ignore some of the optional arguments already specified in the initial submission