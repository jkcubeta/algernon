FROM ubuntu

RUN apt-get update
RUN apt-get upgrade
RUN apt-get install -y python3-pip
RUN apt-get install -y nano
RUN apt-get install -y python3-enchant
RUN pip3 install --upgrade pip

RUN useradd -g users -ms /bin/bash algernon

COPY build/config/requirements.txt /home/algernon/overwatch/requirements.txt

RUN pip3 install -r /home/algernon/overwatch/requirements.txt

EXPOSE 22 80 5672

ENV PATH "$PATH:/home/algernon/bin:/home/algernon/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

COPY /src/overwatch/ /home/algernon/overwatch

WORKDIR /home/algernon

RUN chown -R algernon:users overwatch

USER algernon:users

CMD celery worker -A overwatch.app -E -l INFO