# Resources

## Docker scan for vulnerabilities

After you’ve built an image and before you push your image, run the docker scan command. For detailed instructions on how to scan images using the CLI, see docker scan. <https://docs.docker.com/develop/scan-images/>

## AWS Lambda reusing /tmp

<https://stackoverflow.com/questions/44108712/aws-lambda-release-tmp-storage-after-each-execution>

## Links for Parallel Arch with SQS and Lambda

<https://github-wiki-see.page/m/MatthewMawby/SearchIndex/wiki/Index-Behavior>
<https://1billiontech.com/blog_Building_an_OCR_Backend_with_AWS_Textract_A_Case_Study.php>
<https://data.solita.fi/lessons-learned-from-combining-sqs-and-lambda-in-a-data-project/>

## Fix for wkhtmltopdf: patched qt for multiple page html

```[ERROR] OSError: wkhtmltopdf reported an error:
Error: This version of wkhtmltopdf is build against an unpatched version of QT, and does not support more then one input document.
Exit with code 1, due to unknown error.
    raise IOError('wkhtmltopdf reported an error:\n' + stderr)", line 156, in to_pdfons = {'enable-local-file-access': ''})  
```

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

Fix:

Not fixed yet.