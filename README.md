# ideal-octo-potato
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=active+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=resolved+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)
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
