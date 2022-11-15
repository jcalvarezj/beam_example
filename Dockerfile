FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
COPY . ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/beam_example/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=${WORKDIR}/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

RUN pip install apache-beam[gcp] dnspython
RUN pip install -U -r ./requirements.txt
RUN python setup.py install
