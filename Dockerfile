FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    vim

RUN ln -s /usr/bin/python3 /usr/bin/python && ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip install google-cloud-storage \
    redis

COPY ./bucket-monitor.py /

#CMD ["sleep", "10000"]
CMD ["python", "bucket-monitor.py"]
