SCRIPT_PATH=$(pwd)
pip3 install -r ${SCRIPT_PATH}/requirements.txt
find ${SCRIPT_PATH} -type d -empty -delete