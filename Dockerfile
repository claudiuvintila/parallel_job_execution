FROM continuumio/miniconda3
RUN apt-get update

RUN mkdir /workspace
RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

RUN conda config --add channels conda-forge
RUN conda create -n py39 python=3.9 pip --file /app/requirements.txt
RUN rm /app/requirements.txt

SHELL ["conda", "run", "-n", "py39", "/bin/bash", "-c"]

COPY ./rabbitmq /app/rabbitmq
# Demonstrate the environment is activated:
RUN echo "Make sure pika is installed:"
RUN python -c "import pika"

WORKDIR "/app"
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "py39", "python", "consumer.py"]
