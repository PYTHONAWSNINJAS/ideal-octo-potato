# ideal-octo-potato
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=active+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=resolved+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)
## A docker based solution for AWS Lambda to convert multiple extension files to PDF and put in S3

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

## Build and Push to ECR
Run [build_and_push.py](build_and_push.py) to build the docker image and push to AWS ECR
