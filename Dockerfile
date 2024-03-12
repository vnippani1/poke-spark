FROM datamechanics/spark:3.2.1-hadoop-3.3.1-java-11-scala-2.12-python-3.8-dm18

USER root

WORKDIR /opt/spark

RUN pip install --upgrade pip

COPY  . .
RUN pip3 install -r requirements.txt

EXPOSE 3000
CMD python ./app.py