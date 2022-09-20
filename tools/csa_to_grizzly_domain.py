#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate GRIZZLY domain for CSA scripts export.

Parse https://github.com/GoogleCloudPlatform/security-analytics repository and
generate GRIZZLY configuration YML and SQL files for all analytics scripts in
backends/bigquery/sql folder.

Args:
  grizzly_repo_path: Target GRIZZLY repository folder.
  domain_name: Grizzly domain name.command line
  source_dataset: BQ dataset with exported GCP logs.

Example:
  python3 ./csa_to_grizzly_domain.py \
    -r ~/gh/grizzly/ \
    -d bas_csa \
    -s "my_project.gcp_logging_export"
"""

import argparse
import pathlib
import shutil
import subprocess
import sys
import tempfile
from typing import List
import yaml


class ForeColor:
  """Define text color for terminal output."""
  BLACK = '\033[30m'
  RED = '\033[31m'
  GREEN = '\033[32m'
  YELLOW = '\033[33m'
  BLUE = '\033[34m'
  MAGENTA = '\033[35m'
  CYAN = '\033[36m'
  WHITE = '\033[37m'
  RESET = '\033[39m'
  NONE = '\033[39m'

TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix='csa.'))


def run_command(arguments: List[str]) -> str:
  """Run bash command with arguments and return command output.

  Run bash command. Return command output.

  Args:
    arguments (List[string]): List of strings with definition of command to
      be executed.
      For example: ['gcloud', 'composer', 'environments', 'storage', 'data'].

  Returns:
    (string) Return command output as a text.

  Raises:
    Exception: An error occurred in case if bash command failed.
  """
  cmd_result = subprocess.run(
      arguments,
      check=False,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE)
  if cmd_result.returncode != 0:
    raise Exception(
        f'{ForeColor.RED}{cmd_result.stderr.decode("utf-8")}{ForeColor.RESET}'
    )
  return cmd_result.stdout.decode('utf-8')


def main(args: argparse.Namespace) -> None:
  """Implement the command line interface described in the module doc string."""
  grizzly_path = pathlib.Path(args.grizzly_repo_path, args.domain_name)
  scope_file = pathlib.Path(grizzly_path, 'SCOPE.yml')
  dataset_name = args.domain_name.lower().split('/')[-1]
  # Pull https://github.com/GoogleCloudPlatform/security-analytics to TEMP_DIR
  shell_cmd = [
      'gh', 'repo', 'clone', 'GoogleCloudPlatform/security-analytics',
      TEMP_DIR
    ]
  cmd_result = run_command(shell_cmd)
  csa_scope = {
      'schedule_interval': args.schedule_interval,
      'execution_timeout_per_table': 1200,
      'etl_scope': []
  }
  # Clean up target GRIZZLY repo folder in case of importance
  if grizzly_path.exists():
    shutil.rmtree(grizzly_path)
  grizzly_path.mkdir(exist_ok=True)
  (grizzly_path / 'queries').mkdir(exist_ok=True)
  # Copy CSA scripts to GRIZLY folder
  for f in (TEMP_DIR / 'backends' / 'bigquery' / 'sql').glob('**/*.sql'):
    csa_file_name = dataset_name + '.' + str(f.stem.lower()).split('_', 2)[-1]
    # csa_file_name = f'_{f.stem.lower()}'
    sql_file = pathlib.Path(grizzly_path,
                            'queries',
                            f'{csa_file_name}.sql')
    yml_file = pathlib.Path(grizzly_path,
                            f'{csa_file_name}.yml')
    sql_file_data = f.read_text().replace('[MY_PROJECT_ID].[MY_DATASET_ID]',
                                          args.source_dataset)
    yml_file_data = {
        'target_table_name': csa_file_name,
        'job_write_mode': 'WRITE_TRUNCATE',
        'stage_loading_query': str(sql_file.relative_to(grizzly_path))
    }
    yml_file.write_text(yaml.safe_dump(yml_file_data))
    sql_file.write_text(sql_file_data)
    csa_scope['etl_scope'].append(yml_file.stem)
  # Generate SCOPE YML file.
  scope_file.write_text(yaml.safe_dump(csa_scope))

  return


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(description=__doc__)
    # Add the arguments to the parser
    ap.add_argument(
        '-r',
        '--grizzly_repo_path',
        dest='grizzly_repo_path',
        required=True,
        help='Target GRIZZLY repository folder.')
    ap.add_argument(
        '-d',
        '--domain_name',
        dest='domain_name',
        required=True,
        help='Grizzly domain name.')
    ap.add_argument(
        '-s',
        '--source_dataset',
        dest='source_dataset',
        required=True,
        help='BQ dataset with exported GCP logs.')
    ap.add_argument(
        '--schedule_interval',
        dest='schedule_interval',
        required=True,
        help='GCP Composer schedule interval.')

    main(ap.parse_args())
  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
