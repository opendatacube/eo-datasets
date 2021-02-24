
.PHONY: docker-tests


build:
	docker build -t eodatasets:test .

# Lint and test in one go.
check:
	docker run -it --rm --volume "${PWD}":/tests -w /tests eodatasets:test ./check-code.sh

docker-tests: build
	docker run -it --rm --volume "${PWD}":/tests -w /tests eodatasets:test pytest --cov eodatasets --durations=5

docker-lint: build
	docker run -it --rm --volume "${PWD}":/tests -w /tests eodatasets:test pre-commit run -a

# Interactive shell ready for test running
docker-shell: build
	docker run -it --rm --volume "${PWD}:/tests" -w /tests eodatasets:test /bin/bash
