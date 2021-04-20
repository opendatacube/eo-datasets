
.PHONY: docker-tests


build:
	docker build -t eodatasets:test .

clean-build:
	docker build --no-cache -t eodatasets:test .

# Lint and test in one go.
check:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test ./check-code.sh

test-docker:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test pytest --cov eodatasets3 --durations=5

lint-docker:
	docker run --volume "${PWD}":/tests -w /tests eodatasets:test pre-commit run -a
	# Check the Dockerfile itself?
	# docker run --rm -i hadolint/hadolint < Dockerfile

# Interactive shell ready for test running
shell:
	docker run -it --rm --volume "${PWD}:/tests" --user root -w /tests eodatasets:test /bin/bash

# Old method.
docker-tests: test-docker
	pwd

# Update pinned dependencies. 
# This should be run using the same python interpreter we are deploying with.
# ie. Use "docker-update" instead!
internal-update:
	pip install pip-tools
	pip-compile requirements/dev.in
	pip-compile --extra ancillary --extra wagl -o requirements/main.txt
	pip-compile --extra test -o requirements/test.txt

dependency-update:
	docker run --rm --volume "${PWD}":/tests --user root -w /tests eodatasets:test bash -c 'make internal-update'
