
.PHONY: docker-tests

docker-tests:
	docker build -t eodatasets:test .
	docker run -it --rm --volume "${PWD}/tests":/tests eodatasets:test pytest --cov eodatasets --durations=5 /tests
