# Python support can be specified down to the minor or micro version
# (e.g. 3.6 or 3.6.3).
# OS Support also exists for jessie & stretch (slim and full).
# See https://hub.docker.com/r/library/python/ for all supported Python
# tags from Docker Hub.
FROM python:3.6-slim

LABEL Name=dataset-converter Version=0.0.1

ARG UID=1000
ARG GID=1000

RUN addgroup --system --gid ${GID} user \
&& adduser --system --uid ${UID} --group user

ENV PATH=${PATH}:/home/user/.local/bin

WORKDIR /home/user
ADD ./app /home/user

# Using pip:
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt

ADD ./entrypoint.sh /usr/local/bin
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN chown -R user:user /home/user

USER user

CMD ["entrypoint.sh"]
