FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY main.py .
COPY requirements.txt .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"

RUN pip install apache-beam[gcp] dnspython
RUN pip install -U -r ./requirements.txt