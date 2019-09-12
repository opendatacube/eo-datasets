
.PHONY: docker clean

docker:
	docker build -t opendatacube/eo-datasets:latest .
	docker build -f Dockerfile-test -t opendatacube/eo-datasets:test .
	docker run opendatacube/eo-datasets:test

clean:
	echo "doing nothing"
