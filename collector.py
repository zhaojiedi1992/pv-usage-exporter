from prometheus_client.metrics_core import GaugeMetricFamily
from prometheus_client import Summary
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_http_client import Prometheus

import requests
import json
import jmespath

from aliyunsdkdts.request.v20180801.DescribeSynchronizationJobsRequest import DescribeSynchronizationJobsRequest
from aliyunsdkdts.request.v20180801.DescribeSynchronizationJobStatusRequest import \
    DescribeSynchronizationJobStatusRequest

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException


class DtsStatusCollector():
    def __init__(self):
        self.client = AcsClient('LTAI4Fp8TwZaBvxqQQFHZp2a', '4MWUPcCTQwT9LCnT1XCeacpHHjLgLS', 'cn-hangzhou')

    def format_metric_name(self):
        return 'custom_dts_status_'

    def pager_generator(self, req, page_size, page_num, func):
        req.set_PageSize(page_size)
        while True:
            req.set_PageNum(page_num)
            resp = self.client.do_action_with_exception(req)
            data = json.loads(resp)
            instances = func(data)
            for instance in instances:
                yield instance
            if len(instances) < page_size:
                break
            page_num += 1

    def get_dts_list(self):
        req = DescribeSynchronizationJobsRequest()
        dts_list = list(self.pager_generator(req, page_num=1, page_size=100, func=(
            lambda data: jmespath.search("SynchronizationInstances[].SynchronizationJobId", data))))
        return dts_list

    def get_dts_status(self, dts):
        request = DescribeSynchronizationJobStatusRequest()
        request.set_accept_format('json')
        request.set_SynchronizationJobId(dts)
        resp = self.client.do_action_with_exception(request)
        data = json.loads(resp)
        # result = jmespath.search("Status", data)
        return data

    def collect(self):
        dts_list = self.get_dts_list()
        kv = {
            "name": "SynchronizationJobName",
            "status": "Status",
            "jid": "SynchronizationJobId"
        }
        metrics = {
             "delay": "Delay",
             "flow": "Performance.FLOW",
             "rps": "Performance.RPS",
             "initpercent": "DataInitializationStatus.Percent"
        }

        for dts in dts_list:
            data = self.get_dts_status(dts)
            for metric_key, metric_value in metrics.items():
                gauge = GaugeMetricFamily(self.format_metric_name() + metric_key, '', labels=list(kv.keys()))
                labels = [jmespath.search(v, data) for k, v in kv.items()]
                value = jmespath.search(metric_value,data)
                if value is not None :
                    value = "".join(filter(str.isdigit,value))
                    gauge.add_metric(labels=labels, value=value )
                else:
                    yield self.metric_up_gauge(self.format_metric_name() + metric_key)
                yield gauge

    def metric_up_gauge(self,resource: str, succeeded=True):
        metric_name = resource + '_up'
        description = 'Did the {} fetch succeed.'.format(resource)
        return GaugeMetricFamily(metric_name, description, value=int(succeeded))


if __name__ == "__main__":
    dts = DtsStatusCollector()
    print(dts.collect)
    a = dts.collect()
    print(a)
