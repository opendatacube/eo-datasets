
.PHONY: docker-tests


# Build container
build:
	docker build -t eodatasets:test .

# Full rebuild without the cache (useful periodically to get security updates)
clean-build:
	docker build --no-cache -t eodatasets:test .

# Lint and test in one go.
check:
	docker run --rm --volume "${PWD}":/tests -w /tests eodatasets:test ./check-code.sh

# Run tests in Docker
test:
	docker run --rm --volume "${PWD}":/tests -w /home/runner eodatasets:test pytest --cov eodatasets3 --durations=5 /tests

# Run linters in Docker
lint:
	docker run --volume "${PWD}":/tests -w /tests eodatasets:test pre-commit run -a

# Lint the Dockerfile itself
# (hadolint has too many false positives to run in CI, but is useful for reference)
lint-dockerfile:
	docker run --rm -i hadolint/hadolint < Dockerfile

# Interactive shell, with code mounted to /tests
shell:
	docker run -it --rm --volume "${PWD}:/tests" --user root -w /tests eodatasets:test /bin/bash

# Update pinned dependencies using Docker's python
dependency-update:
	docker run --rm --volume "${PWD}":/tests --user root -w /tests eodatasets:test bash -c 'make internal-update'

# Update dependencies directly
# This has to be run using the same Python interpreter we are deploying with.
# ie. Use "make dependency-update" instead!
internal-update:
	pip-compile requirements/dev.in
	pip-compile --extra ancillary --extra wagl -o requirements/main.txt
	pip-compile --extra test -o requirements/test.txt

