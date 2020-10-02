FROM rafaelleinio/docker-java-python

# setup
COPY . meli-challenge
RUN cd meli-challenge && pip install .

# workdir
WORKDIR /meli-challenge
CMD ["bash"]
