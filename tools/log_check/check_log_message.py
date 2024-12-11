#!/usr/bin/env python3

import json
import os, sys
import subprocess
import tempfile
import re

from argparse import ArgumentParser


def execute_grep(level_msg: str, file: str, pattern: str) -> int:
  out, err = tempfile.TemporaryFile(), tempfile.TemporaryFile()
  p1 = subprocess.Popen(['grep', level_msg, file], stdout=subprocess.PIPE)
  p2 = subprocess.Popen(['grep', pattern], stdin=p1.stdout, stdout=out, stderr=err)
  return p2.wait()


def check(confs: list, starrocks_home: str, be: bool = False) -> bool:
  success = True
  node_type = "be" if be else "fe"
  for conf in confs:
    file, level, pattern = conf["file"], conf["level"], conf["pattern"]
    full_file_path = os.path.join(starrocks_home, node_type, file)
    print (f"Checking {full_file_path}, level {level}, pattern '{pattern}' ...", end='')
    level_msg = f"LOG({level})" if be else f"LOG.{level}"
    if execute_grep(level_msg, full_file_path, pattern) != 0:
      print ("fail")
      success = False
    else:
      print ("success")
  return success


def check_frontend(confs: list, starrocks_home: str) -> bool:
  return check(confs, starrocks_home)


def check_backend(confs: list, starrocks_home: str) -> bool:
  return check(confs, starrocks_home, be=True)


def check_by_pattern(starrocks_home: str, file: str, keywords: str, pattern: re.Pattern) -> bool:
  full_file_path = os.path.join(starrocks_home, "be", file)
  print (f"Checking {full_file_path}, pattern '{keywords}' ...", end='')
  with open(full_file_path, "r") as f:
    text = f.read()
  match = pattern.search(text)
  if match:
    print ("success")
    return True
  else:
    print ("fail")
    return False


def check_rowset_merger_update_compaction_merge(starrocks_home: str) -> bool:
  file = "src/storage/rowset_merger.cpp"
  keywords = "update compaction merge finished"
  pattern = re.compile(r'ss\s*<<\s*"update compaction merge finished.*?duration:.*?ms";\s*if \(st\.ok\(\)\) {\s*LOG\(INFO\) << ss\.str\(\);', re.DOTALL)
  return check_by_pattern(starrocks_home, file, keywords, pattern)


def check_delta_writer_too_many_version(starrocks_home: str) -> bool:
  file = "src/storage/delta_writer.cpp"
  keywords = "because of too many versions"
  pattern = re.compile(r'auto msg = fmt::format\(\s*".*?because of too many versions.*?"\s*,\s*.*?\);\s*LOG\(ERROR\) << msg;', re.DOTALL)
  return check_by_pattern(starrocks_home, file, keywords, pattern)


def check_tablet_updates_too_many_version(starrocks_home: str) -> bool:
  file = "src/storage/tablet_updates.cpp"
  keywords = "because of too many versions"
  pattern = re.compile(r'LOG\(INFO\) << strings::Substitute\(\s*"(.*?because of too many versions\. tablet_id:\$0.*?)"', re.DOTALL)
  return check_by_pattern(starrocks_home, file, keywords, pattern)


def check_compaction_task_compaction_finish(starrocks_home: str) -> bool:
  file = "src/storage/compaction_task.cpp"
  keywords = "compaction finish"
  pattern = re.compile(
    r'std::string\s+msg\s*=\s*strings::Substitute\("compaction\s+finish\. status:\$0,\s+task\s+info:\$1",\s+status\.to_string\(\),\s+_task_info\.to_string\(\)\);\s*'
    r'if\s*\(!status\.ok\(\)\s*\)\s*\{\s*'
    r'LOG\(WARNING\)\s*<<\s*msg;\s*\}\s*else\s*\{\s*'
    r'LOG\(INFO\)\s*<<\s*msg;\s*\}'
  )
  return check_by_pattern(starrocks_home, file, keywords, pattern)


if __name__ == '__main__':
  parser = ArgumentParser()
  parser.add_argument('--home', type=str, required=True, help='StarRocks code home.')
  parser.add_argument('--config', type=str, required=True, help='JSON type configuration, for checking log message.')
  args = parser.parse_args()

  with open(args.config, "r") as f:
    conf = json.load(f)
  success = True
  if "fe" in conf:
    success &= check_frontend(conf["fe"], args.home)
  if "be" in conf:
    success &= check_backend(conf["be"], args.home)
  success &= check_tablet_updates_too_many_version(args.home)
  success &= check_rowset_merger_update_compaction_merge(args.home)
  success &= check_delta_writer_too_many_version(args.home)
  success &= check_compaction_task_compaction_finish(args.home)
  sys.exit(not success)
