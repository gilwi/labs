FROM postgres:13-bullseye

RUN apt-get update && apt-get install -y \
  postgresql-13-pgq3 \
  postgresql-plpython3-13 \
  && rm -rf /var/lib/apt/lists/*
