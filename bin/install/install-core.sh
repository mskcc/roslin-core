#!/bin/bash
script_rel_dir=`dirname ${BASH_SOURCE[0]}`
script_dir=`python3 -c "import os; print(os.path.abspath('${script_rel_dir}'))"`
BIN_DIRECTORY=`python3 -c "import os; print(os.path.abspath(os.path.dirname('${script_dir}')))"`
CONFIG_DIRECTORY=`python3 -c "import os; print(os.path.abspath(os.path.join(os.path.dirname('${script_dir}'),os.path.pardir,'config')))"`
SCHEMA_DIRECTORY=`python3 -c "import os; print(os.path.abspath(os.path.join(os.path.dirname('${script_dir}'),os.path.pardir,'schemas')))"`
# load settings
source $CONFIG_DIRECTORY/settings.sh

mkdir -p ${ROSLIN_CORE_PATH}
mkdir -p ${ROSLIN_CORE_BIN_PATH}
mkdir -p ${ROSLIN_CORE_CONFIG_PATH}
mkdir -p ${ROSLIN_CORE_SCHEMA_PATH}

# copy scripts
cp -r $BIN_DIRECTORY/* ${ROSLIN_CORE_BIN_PATH}
cp -r $CONFIG_DIRECTORY/settings.sh ${ROSLIN_CORE_CONFIG_PATH}
cp -r $SCHEMA_DIRECTORY/* ${ROSLIN_CORE_SCHEMA_PATH}

# give write permission
chmod -R g+w ${ROSLIN_CORE_BIN_PATH}/install

echo "DONE."
