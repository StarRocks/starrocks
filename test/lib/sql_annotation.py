# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

"""
sql_annotation
@Time : 2022/11/2 13:43
@Author : Brook.Ye
"""
from functools import wraps

from nose.plugins.multiprocess import TimedOutException
from cup import log


def timeout():
    """
    timeout exception
    """

    def receive(func):
        """init decorator"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """wrapper"""

            try:
                res = func(*args, **kwargs)
                return res
            except TimedOutException as e:
                raise AssertionError("TimedOutException: exceed the process-timeout limit!")
            except Exception as e:
                raise e

        return wrapper

    return receive


def ignore_timeout():
    """
    ignore timeout exception
    """

    def receive(func):
        """init decorator"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """wrapper"""

            try:
                res = func(*args, **kwargs)
                return res
            except TimedOutException as e:
                log.warning("[Ignore] TimedOutException: exceed the process-timeout limit!")
            except Exception as e:
                raise e

        return wrapper

    return receive
