#!/bin/bash

export ROSLIN_CORE_VERSION="{{ core_version }}"

# path to all the Roslin Core versions are/will be installed
export ROSLIN_CORE_ROOT="{{ roslin_root }}/roslin-core"

# path for a specific version of Roslin Core
export ROSLIN_CORE_PATH="${ROSLIN_CORE_ROOT}/${ROSLIN_CORE_VERSION}"

export ROSLIN_CORE_BIN_PATH="${ROSLIN_CORE_PATH}/bin"
export ROSLIN_CORE_CONFIG_PATH="${ROSLIN_CORE_PATH}/config"

ROSLIN_CORE_SCHEMA_PATH="${ROSLIN_CORE_PATH}/schemas"

export ROSLIN_REDIS_HOST="{{ core_redis_host }}"
export ROSLIN_REDIS_PORT="{{ core_redist_port}}"
