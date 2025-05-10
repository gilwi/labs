FROM python:3.11-slim

WORKDIR /app
COPY producer.py .

RUN pip install faker kafka-python

ENV ES_USE_SSL=true

CMD ["python", "producer.py"]
