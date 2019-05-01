#!/bin/bash

export ROSLIN_CORE_VERSION="{{ core_version }}"

# path to all the Roslin Core versions are/will be installed
export ROSLIN_CORE_ROOT="{{ roslin_core_root }}"

# path for a specific version of Roslin Core
export ROSLIN_CORE_PATH="${ROSLIN_CORE_ROOT}/${ROSLIN_CORE_VERSION}"

export ROSLIN_CORE_BIN_PATH="${ROSLIN_CORE_PATH}/bin"
export ROSLIN_CORE_CONFIG_PATH="${ROSLIN_CORE_PATH}/config"

ROSLIN_CORE_SCHEMA_PATH="${ROSLIN_CORE_PATH}/schemas"

export ROSLIN_MONGO_HOST="{{ core_mongo_host }}"
export ROSLIN_MONGO_PORT="{{ core_mongo_port }}"
export ROSLIN_MONGO_DATABASE="{{ core_mongo_database }}"
export ROSLIN_MONGO_USERNAME="{{ core_mongo_username }}"
export ROSLIN_MONGO_PASSWORD="{{ core_mongo_password }}"

export PATH=$ROSLIN_CORE_BIN_PATH:$PATH
export PATH=$ROSLIN_CORE_BIN_PATH/install:$PATH
export PATH=$ROSLIN_CORE_BIN_PATH/sing:$PATH

echo "Loaded Roslin Core Version ( $ROSLIN_CORE_VERSION )"