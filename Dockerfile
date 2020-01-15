FROM python:2-slim

COPY . /usr/src/elasticstat/
WORKDIR /usr/src/elasticstat/

RUN pip install --no-cache .

ENTRYPOINT [ "python", "./elasticstat/elasticstat.py" ]
CMD [ "--help" ]
