FROM python:2-slim

WORKDIR /usr/src/elasticstat/

COPY ./requirements/prod.txt /usr/src/elasticstat/requirements/
RUN pip install --no-cache -r ./requirements/prod.txt

COPY . /usr/src/elasticstat/
RUN pip install --no-cache .

ENTRYPOINT [ "python", "./elasticstat/elasticstat.py" ]
CMD [ "--help" ]
