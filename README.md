# ideal-octo-potato

## A docker based solution for AWS Lambda to convert multiple extension files to PDF and put in S3

### Steps

1. Install docker
2. Install docker extension in vs code
3. create app folder
4. create app.py in app folder
5. create Dockerfile in main directory
6. place requirements.txt in main directory

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
- [ ] Add code to remove converted files later.
- [ ] Use concurrent processing to reduce time
- [ ] Process for single folder in S3
- [ ] Solve the limit of 512 in /tmp in aws lambda

## Current Blocker

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

Terminal will be reused by tasks, press any key to close it.```

Most probably the IP address/mac address is blocked by public ecr aws site.
Fix: Chnage the base image or Use a different device.
