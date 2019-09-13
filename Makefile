
.PHONY: docker clean

docker:
	docker build -t opendatacube/eo-datasets:latest .
	docker build -f Dockerfile-test -t opendatacube/eo-datasets:test .
	docker run opendatacube/eo-datasets:test

docker_test_args = -e AWS_CA_BUNDLE=/opt/app/keys/ca.pem,CURL_CA_BUNDLE=/opt/app/keys/ca.pem --add-host s3.amazonaws.com:127.0.0.1 --add-host mybucket.s3.amazonaws.com:127.0.0.1 -v `pwd`:/opt/app

test-interactive:
	docker run -it --rm $(docker_test_args) opendatacube/eo-datasets:test /bin/bash

test:
	docker run $(docker_test_args) opendatacube/eo-datasets:test

clean:
	echo "doing nothing"
