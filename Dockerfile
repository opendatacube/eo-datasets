ARG V_BASE=3.3.0

FROM opendatacube/geobase-builder:${V_BASE} as env_builder
ENV LC_ALL=C.UTF-8

ARG py_env_path=/env
ARG ENVIRONMENT=test

# Copy the folder full of requirements-related files in
COPY requirements /tmp/requirements

RUN if [ "$ENVIRONMENT" = "test" ] ; then \
        env-build-tool new /tmp/requirements/docker-dev.txt /tmp/requirements/constraints.txt ${py_env_path} ; \
    else \
        env-build-tool new /tmp/requirements/main.txt /tmp/requirements/constraints.txt ${py_env_path} ; \
    fi

ENV PATH="${py_env_path}/bin:${PATH}"

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN pip install .[all]

# Is it working?
RUN eo3-validate --version

# Build the production runner stage from here
FROM opendatacube/geobase-runner:${V_BASE}
ARG py_env_path=/env

ARG ENVIRONMENT=test

ENV LC_ALL=C.UTF-8 \
    DEBIAN_FRONTEND=noninteractive \
    SHELL=bash \
    PYTHONFAULTHANDLER=1

COPY --from=env_builder $py_env_path $py_env_path
ENV PATH="${py_env_path}/bin:${PATH}"

RUN useradd --create-home runner

RUN if [ "$ENVIRONMENT" = "test" ] ; then \
	apt-get update \
	    # Git for pre-commit linting, make+libpq for pip-tools dependency calc.
	    && apt-get install --no-install-recommends -y git vim make libpq-dev \
	    && rm -rf /var/lib/apt/lists/* ; \
    fi

# Dev setup: run pre-commit once, so its virtualenv is built and cached.
#    We do this in a tmp repository, before copying our real code, as we
#    want this cached by Docker and not rebuilt every time code changes
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

# Is it still working?
RUN eo3-validate --version

CMD ["python"]
