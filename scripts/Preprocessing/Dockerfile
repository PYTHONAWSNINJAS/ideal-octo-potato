# Base image
FROM amazon/aws-lambda-python:3.8

USER root

WORKDIR /usr/app

ADD . '/usr/app'

RUN pip install flask
RUN pip install boto3

EXPOSE 5000

CMD ["python", "app.py"]