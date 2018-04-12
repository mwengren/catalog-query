FROM python:3-stretch

WORKDIR /srv/catalog-query

COPY . .

RUN pip install -e .

ENTRYPOINT ["catalog-query"]
