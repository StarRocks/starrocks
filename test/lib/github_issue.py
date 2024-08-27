#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
#
# Copyright (c) 2020 Copyright (c) 2020, Dingshi Inc.  All rights reserved.
#
###########################################################################
"""
github issue api, used for issue operation
"""
import os

import requests
from github import Github

# Authentication is defined via GitHub.Auth
ISSUE_URL = os.environ.get('ISSUE_URL')


class GitHubApi:
    @staticmethod
    def create_issue(title, body, label, assignee):

        if not ISSUE_URL:
            return 'Skip the task of creating issues!'

        issue_json = {
            'title': title,
            'body': body,
            'label': label,
            'assign': assignee
        }
        header = {
            'Content-Type': 'application/json'
        }
        try:
            create_res = requests.post(ISSUE_URL, headers=header, json=issue_json)
            if create_res.status_code == 200:
                return create_res.text
            else:
                return f"Create issue error, [{create_res.status_code}] {create_res.text}"

        except Exception as e:
            return "Create issue error, Exception: %s" % e
