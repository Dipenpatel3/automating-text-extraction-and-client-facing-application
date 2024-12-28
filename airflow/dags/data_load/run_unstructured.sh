#!/bin/bash

# Echo to show the process has started
echo 'Starting the Bash script and importing variables from Python'


if [ -f /tmp/env_file.sh ]; then
  source /tmp/env_file.sh  # This file was generated by the Python script
else
  echo "Environment file /tmp/env_file.sh not found!"
  exit 1
fi

# Import variables from Python using absolute path
eval $(python3 /opt/airflow/dags/data_load/parameter_config_airflow.py)


# Now, run your Python pipeline script
python /opt/airflow/dags/data_load/pdf_extraction_unstructured.py

# Echo to indicate the process has completed
echo 'Python script executed successfully'


if [ -f /tmp/env_file.sh ]; then
  rm /tmp/env_file.sh
  echo "Environment file /tmp/env_file.sh removed."
else
  echo "Environment file /tmp/env_file.sh not found for removal."
fi