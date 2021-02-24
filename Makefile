
.PHONY: docker-tests

docker-tests:
	docker build -t eodatasets:test .
	docker run -it --rm --volume "${PWD}":/tests -w /tests eodatasets:test pytest --cov eodatasets --durations=5

# Interactive shell ready for test running
docker-shell:
	docker run -it --rm --volume ${PWD}:/tests -w tests eodatasets:test /bin/bash
