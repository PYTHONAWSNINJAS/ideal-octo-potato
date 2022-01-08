# ideal-octo-potato
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=active+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)
[![DeepSource](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato.svg/?label=resolved+issues&show_trend=true&token=whwikFGIu8kkgj8AMfh_5BLD)](https://deepsource.io/gh/PYTHONAWSNINJAS/ideal-octo-potato/?ref=repository-badge)

A docker based solution for AWS Lambda to convert multiple extension files to PDF and put in S3

## Components
### AWS S3 Bucket
1. Main S3 Bucket contains all case folders placed in wire and exhibits folders. The stucture is as follows:
```
```
2. Trigger S3 bucket - This bucket is used to trigger the main lambda on put file event of document folders. Ex -
3. Merge Trigger S3 Bucket -  This bucket is used to trigger the merge lambda on put file event of control files. Ex -
4. Metadata S3 Bucket - This is used for metadata files which keeps the count of files to be processed and number of success files.

### AWS Lambda
1. Main Lambda - This conversion of doucments to pdf is done here.
2. Merge Lambda -  The converted pdfs are merged into Source and Current pdfs in this function.
3. Doc Processing Lambda - This is a sub function of the main lambda. This lambda converts doc/docx files to pdf.
4. Postprocessing Lambda - This checks the RDS and marks a case folder as complete when all the files are processed.

### ECS Fargate
This runs a preprocessing code at the start of the process and sends out trigger files to Trigger S3 Bucket.

### AWS Elastic Container Registry
This stores the images of preprocessing and main lambda code as docker images.

### AWS Elastic File System
This is incorporated to be used with the lambda functions as lambda has a limit of 512 MB of RAM.

### RDS
This stores information about the ongoing activity.

### VPC and Security Groups


### Payload example




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
