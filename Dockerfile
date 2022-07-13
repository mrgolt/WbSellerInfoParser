FROM python:3.8

ADD . /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

RUN apt-get update
RUN apt-get -y install locales
RUN locale-gen ru_RU.UTF-8

CMD ["python", "-u", "./run.py"]