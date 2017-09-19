FROM ubuntu

RUN apt-get update
RUN apt-get upgrade
RUN apt-get install -y python3-pip
RUN apt-get install -y python3-enchant
RUN pip3 install --upgrade pip

RUN useradd -g users -ms /bin/bash algernon

COPY build/requirements.txt /home/algernon/alg_py/requirements.txt

RUN pip3 install -r /home/algernon/alg_py/requirements.txt

EXPOSE 22 80 5672

ENV NAME World
ENV PATH "$PATH:/home/algernon/bin:/home/algernon/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV FLASK_APP=alg_api

COPY . /home/algernon/alg_py

WORKDIR /home/algernon/

RUN chown -R algernon:users alg_py

USER algernon:users

CMD flask run --host=0.0.0.0