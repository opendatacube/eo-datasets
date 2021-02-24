
.PHONY: docker-tests


build:
	docker build -t eodatasets:test .

# Lint and test in one go.
check:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test ./check-code.sh

test-docker:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test pytest --cov eodatasets3 --durations=5

lint-docker:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test pre-commit run -a

# Interactive shell ready for test running
shell:
	docker run -it --rm --volume "${PWD}:/tests" -w /tests eodatasets:test /bin/bash

# Old method.
docker-tests: test-docker
	pwd
