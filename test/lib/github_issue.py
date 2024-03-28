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
from github import Github

# Authentication is defined via GitHub.Auth
# using an access token
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
# Public Web GitHub
gh = Github(GITHUB_TOKEN)


class GitHubApi:
    def __init__(self, repo):
        # default is StarRocks/StarRocks
        self.repo = gh.get_repo(repo)

    def create_issue(self, title, body, label, assignee):
        try:
            self.repo.create_issue(title=title, body=body, labels=[label], assignee=assignee)
        except Exception as e:
            print("Create issue error, Exception: %s" % e)


if __name__ == "__main__":
    pass
