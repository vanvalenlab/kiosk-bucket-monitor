FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

RUN ln -s /usr/bin/python3 /usr/bin/python && ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip install google-cloud-storage

COPY ./bucket-monitor.py /
COPY DeepCell-abd537822d08.json /

#CMD ["bash"]
CMD ["python", "bucket-monitor.py"]
