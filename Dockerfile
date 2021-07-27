# FROM public.ecr.aws/lambda/python:3.8
FROM amazon/aws-lambda-python:3.8
 
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt 

# Required for pytesseract
RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum -y update
RUN yum -y install tesseract

# Required for pdfkit
RUN yum -y install openssl build-essential xorg libssl-dev
RUN yum -y install wkhtmltopdf
RUN yum -y install xorg-x11-server-Xvfb
RUN printf '#!/bin/bash\nxvfb-run -a --server-args="-screen 0, 1024x768x24" /usr/bin/wkhtmltopdf -q $*' > /usr/bin/wkhtmltopdf.sh
RUN chmod a+x /usr/bin/wkhtmltopdf.sh
RUN ln -s /usr/bin/wkhtmltopdf.sh /usr/local/bin/wkhtmltopdf

COPY ./app/app.py   ./
 
RUN aws configure set aws_access_key_id "AKIAWFP72WY7VYVFY76G"
RUN aws configure set aws_secret_access_key "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"
RUN aws configure set region "us-east-1"
RUN aws configure set output "text"

CMD ["app.lambda_handler"]