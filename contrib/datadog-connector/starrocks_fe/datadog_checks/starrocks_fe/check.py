# (C) Datadog, Inc. 2022-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from copy import deepcopy

from datadog_checks.base import OpenMetricsBaseCheck

# from datadog_checks.base.utils.db import QueryManager
# from requests.exceptions import ConnectionError, HTTPError, InvalidURL, Timeout
# from json import JSONDecodeError
from datadog_checks.starrocks_fe.metrics import FE_METRICS
from datadog_checks.starrocks_fe.utils import build_check



class StarrocksFeCheck(OpenMetricsBaseCheck):

    # This will be the prefix of every metric and service check the integration sends
    __NAMESPACE__ = 'starrocks_fe'

    def __init__(self, name, init_config, instances):
        # This will be the prefix of every metric and service check the integration sends
        openmetrics_instance = deepcopy(instances[0])

        build_check("fe", openmetrics_instance)

        default_instances = {
            self.__NAMESPACE__: build_check(
                "fe",
                {
                    'fe_metric_url': 'http://localhost:8030/metrics',
                    'metrics': FE_METRICS,
                }

            ),
        }

        super(StarrocksFeCheck, self).__init__(
            name,
            init_config,
            [openmetrics_instance],
            default_instances=default_instances,
            default_namespace=self.__NAMESPACE__,
        )