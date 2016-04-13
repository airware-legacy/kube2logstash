# kube2logstash

This is just small daemon for shipping kubernetes events into logstash.

## Use Cases

- Watch for an outlier increase in `Killing` events indicating unhealthy pods
- Cheap metrics on cluster status
