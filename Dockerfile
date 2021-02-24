FROM opendatacube/geobase:wheels as env_builder
ARG py_env_path=/env
ARG ENVIRONMENT=test

COPY requirements*.txt /tmp/
# RUN env-build-tool new /tmp/requirements.txt ${py_env_path}
RUN if [ "$ENVIRONMENT" = "test" ] ; then \
        env-build-tool new /tmp/requirements-test.txt ${py_env_path} ; \
    else \
        env-build-tool new /tmp/requirements.txt ${py_env_path} ; \
    fi

ENV PATH=${py_env_path}/bin:$PATH

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN pip install --use-feature=2020-resolver .

# Make sure it's working first
RUN eo3-validate --help

# Build the production runner stage from here
FROM opendatacube/geobase:runner

ENV LC_ALL=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    SHELL=bash

COPY --from=env_builder /env /env
ENV PATH=/env/bin:$PATH

RUN apt-get update \
    # Git is needed for pre-commit linting
    && apt-get install -y git vim \
    && rm -rf /var/lib/apt/lists/*

#  # Environment can be whatever is supported by setup.py
#  # so, either deployment, test
#  ARG ENVIRONMENT=test
#  RUN echo "Environment is: $ENVIRONMENT"
#
#  # Set up a nice workdir, and only copy the things we care about in
#  ENV APPDIR=/code
#  RUN mkdir -p $APPDIR
#  WORKDIR $APPDIR
#  ADD . $APPDIR
#
#  # These ENVIRONMENT flags make this a bit complex, but basically, if we are in dev
#  # then we want to link the source (with the -e flag) and if we're in prod, we
#  # want to delete the stuff in the /code folder to keep it simple.
#  RUN if [ "$ENVIRONMENT" = "deployment" ] ; then rm -rf $APPDIR ; \
#      else pip install --editable .[$ENVIRONMENT] ; \
#      fi

RUN python

CMD ["python"]
