FROM rafaelleinio/docker-java-python

COPY ./requirements.txt /legiti-challenge/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /legiti-challenge/requirements.txt

COPY . /legiti-challenge
RUN pip install /legiti-challenge/.

WORKDIR /legiti-challenge
ENTRYPOINT ["python", "./legiti_challenge/cli.py"]
