from prometheus_client.metrics_core import GaugeMetricFamily
from prometheus_client import Summary
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_http_client import Prometheus

import requests
import json
import jmespath


class PersistentVolumeUsageCollector():
    def __init__(self):
        self.a=1
        self.b=2
    def format_metric_name(self):
        return 'custom_pv_'

    def get_nodes_instance(self):
        promethus = Prometheus()
        query_result = json.loads(promethus.query(metric="kubelet_running_pod_count"))
        instances = jmespath.search('data.result[].metric.instance', query_result)
        instances = list(set(instances))

        return instances
        #return ["172.16.21.198:10255"]

    def get_volume_info(self, instance):
        url = "http://{0}/stats/summary".format(instance)
        req = requests.get(url)
        if req.status_code == 200:
            res_json = req.json()

            sub_metric_items = [
                "availableBytes",
                "capacityBytes",
                "usedBytes",
                "inodesFree",
                "inodes",
                "inodesUsed"
            ]
            for sub_metric in sub_metric_items:
                for pod in res_json["pods"]:
                    if not "volume" in pod :
                        continue
                    pod_volumes = pod["volume"]
                    pod_pod_ref = pod["podRef"]
                    for volume in pod_volumes :
                        kv = {
                            "instance": instance,
                            "namespace": pod_pod_ref["namespace"],
                            "pod": pod_pod_ref["name"],
                            "name": volume["name"]
                        }
                        gauge = GaugeMetricFamily(self.format_metric_name() + sub_metric, '',labels=list(kv.keys()))
                        gauge.add_metric(labels=list(kv.values()), value=volume[sub_metric])
                        yield  gauge
                        #yield self.metric_up_gauge(self.format_metric_name() + sub_metric, True)

    def collect(self):
        print("abc")
        instances = self.get_nodes_instance()
        for instance in instances:
            yield from  self.get_volume_info(instance)

    def metric_up_gauge(resource: str, succeeded=True):
        metric_name = resource + '_up'
        description = 'Did the {} fetch succeed.'.format(resource)
        return GaugeMetricFamily(metric_name, description, value=int(succeeded))


if __name__ == "__main__":
    pv = PersistentVolumeUsageCollector()
    print(pv.collect)
    a=pv.collect()
    print(a)
