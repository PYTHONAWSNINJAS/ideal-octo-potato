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
2. aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/b9x5z8y4
3. docker tag <docker_image_id> ecr_uri
4. docker push ecr_uri


## ToDo:

- [x] Install tesseract for amazonlinux2
- [x] Change to S3 code
- [x] Test by pulling from S3 and Upload back to the same location
- [x] Make container image
- [x] Deploy to Elastic Container Registry
- [x] Deploy to lambda
- [ ] Test ECR image with lambda function - "The config profile (lambdauser) could not be found", but is present in image aws configure list -p lambdauser
- [ ] Use concurrent processing to reduce time
- [ ] Process for single folder in S3