FROM ubuntu

RUN apt-get update
RUN apt-get upgrade
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pip

RUN useradd -g users -ms /bin/bash algernon

COPY build/config/requirements.txt /home/algernon/api/requirements.txt

RUN pip3 install -r /home/algernon/api/requirements.txt

EXPOSE 80

ENV PATH "$PATH:/home/algernon/bin:/home/algernon/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV FLASK_APP=alg_api

COPY /src/ /home/algernon/api

WORKDIR /home/algernon/

RUN chown -R algernon:users api

USER algernon:users

CMD flask run --host=0.0.0.0