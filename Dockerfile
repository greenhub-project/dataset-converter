# Python support can be specified down to the minor or micro version
# (e.g. 3.6 or 3.6.3).
# OS Support also exists for jessie & stretch (slim and full).
# See https://hub.docker.com/r/library/python/ for all supported Python
# tags from Docker Hub.
FROM python:3.7-slim

LABEL Name=dataset-converter Version=0.2.1
LABEL maintainer="Hugo Matalonga <dev@hmatalonga.com>"

ARG UID=1000
ARG GID=1000

RUN apt-get update \
&& apt-get install -y --no-install-recommends p7zip-full \
&& rm -rf /var/lib/apt/lists/*

RUN addgroup --system --gid ${GID} user \
&& adduser --system --uid ${UID} --group user

WORKDIR /home/user
COPY ./app/requirements.txt /home/user

RUN python3 -m pip install --no-cache-dir --compile -r requirements.txt

ADD ./entrypoint.sh /usr/local/bin
RUN chmod +x /usr/local/bin/entrypoint.sh

COPY ./app/ /home/user/
RUN chown -R user:user /home/user

USER user
ENV PATH=${PATH}:/home/user/.local/bin

CMD ["/usr/local/bin/entrypoint.sh"]
