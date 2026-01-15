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
import signal


def timeout(seconds):

    def receive(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            def _handler(signum, frame):
                raise TimeoutError(f"Timed out after {seconds}s")

            old_handler = signal.getsignal(signal.SIGALRM)
            try:
                signal.signal(signal.SIGALRM, _handler)
                signal.alarm(seconds)
                res = func(*args, **kwargs)
                return res
            except TimeoutError as e:
                log.error(e)
                raise e
            except TimedOutException as e:
                log.error(f"TimedOutException: exceed the process-timeout limit! {e}")
                raise AssertionError("TimedOutException: exceed the process-timeout limit!")
            except Exception as e:
                raise e
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

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
