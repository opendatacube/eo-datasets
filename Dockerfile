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

RUN useradd --create-home runner

COPY --from=env_builder /env /env
ENV PATH=/env/bin:$PATH

RUN if [ "$ENVIRONMENT" = "test" ] ; then \
	apt-get update \
	    # Git for pre-commit linting, make+libpq for pip-tools dependency calc.
	    && apt-get install --no-install-recommends -y git vim make libpq-dev \
	    && rm -rf /var/lib/apt/lists/* ; \
    fi

# For dev: run pre-commit once, so its environment is built and cached.
#    We make a little tmp repo rather than using our real repo, as we only
#    want Docker's caching to rebuild it when pre-commit-config.yaml changes.
COPY .pre-commit-config.yaml /tmp/
USER runner
RUN if [ "$ENVIRONMENT" = "test" ] ; then \
       mkdir -p ~/pre-commit \
       && cp /tmp/.pre-commit-config.yaml ~/pre-commit \
       && cd ~/pre-commit \
       && git init \
       && pre-commit run \
       && rm -rf ~/pre-commit ; \
    fi

# Copy source code and install it
WORKDIR /code
COPY . .

USER root
RUN pip install --no-cache-dir --disable-pip-version-check --use-feature=2020-resolver .
USER runner

# Is it working?
RUN eo3-validate --version

CMD ["python"]
