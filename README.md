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
```
```

### Process
The process starts as soon as an external system requests the api with the above mentioned payload which contains the case number. The payload is accepted by the function running in fargate and it then starts listing the control files. It places an rds entry of the number of cotrol files found for that case. Next the function loops over wire and exhibits folder both and starts placing metadata file in metadata s3 bucket and 0 byte trigger files for each document folder using concurrently running threads in the processor. And just before ending this process, we enable the cloudwatch rule.

As soon as the S3 trigger files are placed, the main lambda is triggered. This function starts by downloading the files in efs. It sequencially processes all the files in the folder and places them in doc_pdf location. Next after sucessfull processing, it deletes the trigger object from the s3 trigger bucket and creates a success object in metadata s3 bucket. Now when the function has done processing it counts the metadata object and the number of success files in the bucket. If the condition matches, it puts a control file object as merge lambda trigger in merge s3 trigger bucket. And before ending the execution it deletes all converted files from efs.

The merge trigger file invokes the merge lambda where the control files is read to get all the list of files that are to be stitched into one single file. The converted pdf files are downloaded into efs and merged and uploaded back to main s3 as source and current. The metadata file and merge trigger bucket files are deleted once the process is completed. The process also updates the rds and increments the value. It clears the efs for any files present. 

The post processing lambda keeps running in specific time intervals and keeps a check on the number of processed documents. Once total_triggers is equal to processed_triggers, it removeds the entry from RDS and places a COMPLETED file in the case folder marking it complete. Further if it sees the table is empty it also disables the cloudwatch rule.

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
