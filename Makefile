.PHONY: docker-build build

docker-build:
	go build --ldflags '-extldflags "-static"'

build:
	docker build -t kube2logstash_builder -f Dockerfile.build .
	-docker rm k2l_builder
	docker run -i --name k2l_builder kube2logstash_builder
	mkdir -p build/
	docker cp k2l_builder:/go/src/github.com/airware/kube2logstash/kube2logstash build/kube2logstash
	docker build -t kube2logstash .

publish:
	docker tag -f kube2logstash quay.io/airware/kube2logstash
	docker push quay.io/airware/kube2logstash
