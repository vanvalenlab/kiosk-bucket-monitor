FROM python:3.6

WORKDIR /usr/src/app

COPY requirements.txt .

RUN pip install requirements.txt

COPY . .

CMD ["python", "bucket-monitor.py"]
