FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt 

RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y update
RUN yum -y install tesseract

COPY ./app/app.py   ./
 
RUN aws configure set aws_access_key_id "AKIAWFP72WY7VYVFY76G"
RUN aws configure set aws_secret_access_key "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"
RUN aws configure set region "us-east-1"
RUN aws configure set output "text"

CMD ["app.lambda_handler"]