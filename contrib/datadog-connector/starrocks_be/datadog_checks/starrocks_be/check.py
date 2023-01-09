# (C) Datadog, Inc. 2022-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from copy import deepcopy

from datadog_checks.base import OpenMetricsBaseCheck

# from datadog_checks.base.utils.db import QueryManager
# from requests.exceptions import ConnectionError, HTTPError, InvalidURL, Timeout
# from json import JSONDecodeError
from datadog_checks.starrocks_be.metrics import BE_METRICS
from datadog_checks.starrocks_be.utils import build_check


class StarrocksBeCheck(OpenMetricsBaseCheck):

    # This will be the prefix of every metric and service check the integration sends
    __NAMESPACE__ = 'starrocks_be'

    def __init__(self, name, init_config, instances):
        openmetrics_instance = deepcopy(instances[0])

        build_check("be", openmetrics_instance)

        default_instances = {
            self.__NAMESPACE__: build_check(
                "be",
                {
                    'be_metric_url': 'http://localhost:8040/metrics',
                    'metrics': BE_METRICS,
                }

            ),
        }

        super(StarrocksBeCheck, self).__init__(
            name,
            init_config,
            [openmetrics_instance],
            default_instances=default_instances,
            default_namespace=self.__NAMESPACE__,
        )