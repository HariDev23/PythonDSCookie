import os
import shutil

print(os.getcwd())  # prints /absolute/path/to/{{cookiecutter.project_slug}}

def remove(filepath):
    if os.path.isfile(filepath):
        os.remove(filepath)
    elif os.path.isdir(filepath):
        shutil.rmtree(filepath)

need_ga4api_utils = '{{cookiecutter.need_ga4api_utils}}' == 'y'
need_snowflake_utils = '{{cookiecutter.need_snowflake_utils}}' == 'y'
need_bigquery_utils = '{{cookiecutter.need_bigquery_utils}}' == 'y'
need_postgress_utils = '{{cookiecutter.need_postgress_utils}}' == 'y'
need_SQLserver_utils = '{{cookiecutter.need_SQLserver_utils}}' == 'y'

if not need_ga4api_utils:
    # remove top-level file inside the generated folder
    remove(os.path.join('data', 'utils', 'ga4api'))

if not need_snowflake_utils:
    # remove top-level file inside the generated folder
    remove(os.path.join('data', 'utils', 'snowflake'))

if not need_snowflake_utils:
    # remove top-level file inside the generated folder
    remove(os.path.join('data', 'utils', 'bigquery'))

if not need_snowflake_utils:
    # remove top-level file inside the generated folder
    remove(os.path.join('data', 'utils', 'postgres'))

if not need_snowflake_utils:
    # remove top-level file inside the generated folder
    remove(os.path.join('data', 'utils', 'SQLserver'))

