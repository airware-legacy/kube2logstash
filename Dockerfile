FROM alpine:3.3

COPY build/kube2logstash /kube2logstash

CMD ["/kube2logstash"]
