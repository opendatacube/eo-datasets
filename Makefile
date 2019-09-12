
.PHONY: docker clean

docker:
	docker build -t opendatacube/eo-datasets:latest .
	docker build -f Dockerfile-test -t opendatacube/eo-datasets:test .
	docker run opendatacube/eo-datasets:test

test-interactive:
	docker run -it --rm -v `pwd`:/opt/app opendatacube/eo-datasets:test /bin/bash

clean:
	echo "doing nothing"
