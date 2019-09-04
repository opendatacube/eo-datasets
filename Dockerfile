FROM continuumio/miniconda3

COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app


# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

RUN conda config --add channels conda-forge \
    && conda config --set channel_priority strict
RUN conda create -n odc --file requirements.txt

COPY . /opt/app
SHELL ["/bin/bash", "-cl"]
RUN conda activate odc && pip install .[ancillary]

RUN sed -i 's/conda activate base/conda activate odc/g' ~/.bashrc


WORKDIR /opt/odc
ENTRYPOINT ["/bin/bash", "-lc"]
