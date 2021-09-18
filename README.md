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

- [ ] Apply Black Code for optimisation - <https://github.com/psf/black>
- [ ] Test with xlrd instead of openpyxl
- [ ] Change to cross-platform tmp directory with tempfile module
- [ ] Add docx and doc
- [ ] Explore docker sync
- [ ] check eml files. wkhtmltopdf reported an error: Error: This version of wkhtmltopdf is build against an unpatched version of QT, and does not support more then one input document. Exit with code 1, due to unknown error.
- [ ] Check and optimise the flow, names and deletion activities.
- [ ] Add log creation to S3 based on trigger folder 
```
Use tempfile and create a log and put all info in the file.
Transfer the file to S3 in 2021/09/16/TriggerFolderName.log 
```
- [ ] Stitching all pdf files based on control file -  format

```
TBD
{
    "1011":{
        "1600094973-2055.47140682846840":[
            {
                "page1":"file_1"
            },
            {
                "page2":"file_2"
            }
        
        ],
        "1600094973-2055.47140682846841":[
            {
                "page1":"file_3"
            },
            {
                "page2":"file_4"
            }
        
        ]
    },
    "10452":[

    ],
    "115":[

    ]
}
```