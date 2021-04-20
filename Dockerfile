FROM opendatacube/geobase:wheels as env_builder
ARG py_env_path=/env
ARG ENVIRONMENT=test

COPY requirements /tmp/requirements
# RUN env-build-tool new /tmp/requirements.txt ${py_env_path}
RUN if [ "$ENVIRONMENT" = "test" ] ; then \
        rm /wheels/rasterio*whl ; \
        env-build-tool new /tmp/requirements/docker-dev.txt ${py_env_path} ; \
    else \
        env-build-tool new /tmp/requirements/main.txt ${py_env_path} ; \
    fi

ENV PATH=${py_env_path}/bin:$PATH


# Build the production runner stage from here
FROM opendatacube/geobase:runner
ARG ENVIRONMENT=test

ENV LC_ALL=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    SHELL=bash \
    PYTHONFAULTHANDLER=1

COPY --from=env_builder /env /env
ENV PATH=/env/bin:$PATH

RUN if [ "$ENVIRONMENT" = "test" ] ; then \
	apt-get update \
	    # Git is needed for pre-commit linting
	    && apt-get install --no-install-recommends -y git vim \
	    && rm -rf /var/lib/apt/lists/* ; \
    fi

RUN useradd --create-home runner

# Copy source code and install it
WORKDIR /code
COPY . .

RUN pip install --no-cache-dir --disable-pip-version-check --use-feature=2020-resolver .

USER runner

# Is it working?
RUN eo3-validate --version

CMD ["python"]
