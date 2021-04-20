
.PHONY: docker-tests


build:
	docker build -t eodatasets:test .

clean-build:
	docker build --no-cache -t eodatasets:test .

# Lint and test in one go.
check:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test ./check-code.sh
	# Check the Dockerfile itself?
	# docker run --rm -i hadolint/hadolint < Dockerfile

test:
	docker run --rm --volume "${PWD}":/tests -w /home/runner eodatasets:test pytest --cov eodatasets3 --durations=5 /tests

lint:
	docker run --volume "${PWD}":/tests -w /tests eodatasets:test pre-commit run -a

# Interactive shell ready for test running
shell:
	docker run -it --rm --volume "${PWD}:/tests" --user root -w /tests eodatasets:test /bin/bash

# Update pinned dependencies. 
# This should be run using the same python interpreter we are deploying with.
# ie. Use "docker-update" instead!
internal-update:
	pip-compile requirements/dev.in
	pip-compile --extra ancillary --extra wagl -o requirements/main.txt
	pip-compile --extra test -o requirements/test.txt

dependency-update:
	docker run --rm --volume "${PWD}":/tests --user root -w /tests eodatasets:test bash -c 'make internal-update'
