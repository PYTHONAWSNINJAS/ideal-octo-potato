# ideal-octo-potato

## A docker based solution for AWS Lambda to convert multiple extension files to PDF and put in S3

### Steps

1. Install docker
2. Install docker extension in vs code
3. create app folder
4. create app.py in app folder
5. create Dockerfile in main directory
6. place requirements.txt in main directory

## AWS IAM Permissions needed

```S3:ListBucket
S3:GetObjects
S3:PutObjects
ecr:GetAuthorizationToken
```

## Fix for GetAuthorizationToken exception

configure aws credentials

## Docker Commands

### Build

```docker build -t <image-name> .```
or right click -> build image from Dockerfile

### Images

```docker images```

### Run

```docker run -p 9000:8080 <image-name>```

### Test

(From another terminal)
```curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "{"""msg""":"""hello"""}"```
```curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d "{}"```

The docker terminal will display server side messages. The new terminal will display the client side messages.

## AWS CLI Commands

### Displays all the profile related info

These two files store the info:

1. credentials
2. config

Note: clear the credentials and set these.

## Push to ECR

1. docker images
2. aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
3. docker tag <docker_image_id> ecr_uri
4. docker push ecr_uri

## ToDo

- [x] Install tesseract for amazonlinux2
- [x] Change to S3 code
- [x] Test by pulling from S3 and Upload back to the same location
- [x] Make container image
- [x] Deploy to Elastic Container Registry
- [x] Deploy to lambda
- [x] Test ECR image with lambda function
- [x] Add code to remove converted files later.
- [ ] Use concurrent processing to reduce time
- [x] Process for single folder in S3
- [x] Use env variables for bucketname, sub folder and document name and credentials
- [ ] Solve the limit of 512 MB in /tmp in aws lambda
- [ ] Apply Black Code for optimisation - <https://github.com/psf/black>
- [ ] Test with xlrd instead of openpyxl
- [x] Dump into another folder in exhibits level (use env variable) doc_pdf only the pdf files
- [x] For a pdf <name>.pdf, rename to <name>_dv.pdf

## Fix for failed to solve with frontend dockerfile.v0

Details:

```[+] Building 9.6s (4/4) FINISHED
 => [internal] load build definition from Dockerfile                                                                       0.5s 
 => => transferring dockerfile: 32B                                                                                        0.0s 
 => [internal] load .dockerignore                                                                                          0.8s 
 => => transferring context: 2B                                                                                            0.0s 
 => ERROR [internal] load metadata for public.ecr.aws/lambda/python:3.8                                                    8.5s 
 => [auth] aws:: lambda/python:pull token for public.ecr.aws                                                               0.0s 
------
 > [internal] load metadata for public.ecr.aws/lambda/python:3.8:
------
failed to solve with frontend dockerfile.v0: failed to create LLB definition: unexpected status code [manifests 3.8]: 403 Forbidden
The terminal process "C:\Windows\System32\cmd.exe /d /c docker build --pull --rm -f "Dockerfile" -t idealoctopotato:latest "."" terminated with exit code: 1.

Terminal will be reused by tasks, press any key to close it.
```

Fix:
Most probably the IP address/mac address is blocked by public ecr aws site.
Fix: Change the base image or Use a different device.

## Fix for wkhtmltopdf: cannot connect to X server

```You will need to run wkhtmltopdf within a "virtual" X server.
Go to the link below for more information
https://github.com/JazzCore/python-pdfkit/wiki/Using-wkhtmltopdf-without-X-server
```

Fix:

```yum install xorg-x11-server-Xvfb
printf '#!/bin/bash\nxvfb-run -a --server-args="-screen 0, 1024x768x24" /usr/bin/wkhtmltopdf -q $*' > /usr/bin/wkhtmltopdf.sh
chmod a+x /usr/bin/wkhtmltopdf.sh
ln -s /usr/bin/wkhtmltopdf.sh /usr/local/bin/wkhtmltopdf
```

If you cannot acquire the root shell (e.g. on an Azure/AWS Devops Agent) change the third line to:

```printf '#!/bin/bash\nxvfb-run -a --server-args="-screen 0, 1024x768x24" /usr/bin/wkhtmltopdf -q $*' | sudo tee /usr/bin/wkhtmltopdf.sh
```

## Fix for wkhtmltopdf: patched qt for multiple page html

```[ERROR] OSError: wkhtmltopdf reported an error:
Error: This version of wkhtmltopdf is build against an unpatched version of QT, and does not support more then one input document.
Exit with code 1, due to unknown error.
    raise IOError('wkhtmltopdf reported an error:\n' + stderr)", line 156, in to_pdfons = {'enable-local-file-access': ''})  
```

Fix:

Not fixed yet.

## For db file

import pandas as pd
import pdfkit as pdf
import sqlite3

con=sqlite3.connect("baza.db")

df=pd.read_sql_query("select * from dobit", con)
df.to_html('/home/linux/izvestaj.html')
nazivFajla='/home/linux/pdfPrintOut.pdf'
pdf.from_file('/home/linux/izvestaj.html', nazivFajla)
