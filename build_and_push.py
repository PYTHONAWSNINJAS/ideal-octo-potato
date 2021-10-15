"""
This module will run locally to build a docker image 
using the app.py under app folder using the Dockerfile
and install the modules in requirements.txt.
Next, it will Login to AWS and Push the Image to 
ECR Repository.
"""

import os


def build(account_id, image_name, repository_name, docker_file):
    """
    Parameters
    ----------
    account_id: AWS account ID
    image_name: Docker image to be built and assigned a name
    repository_name: AWS ECR repository name
    """
    print("starting docker build...")
    os.system("docker build --pull --rm -f \""+docker_file+"\" -t " + image_name + ":latest \".\"")
    print("done.")

    print("logging in aws ecr...")
    os.system(
        "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin " + account_id +
        ".dkr.ecr.us-east-1.amazonaws.com")
    print("done.")

    print("tagging...")
    os.system(
        "docker tag " + image_name + ":latest " + account_id + ".dkr.ecr.us-east-1.amazonaws.com/" +
        repository_name)
    print("done.")

    print("pushing to ecr...")
    os.system("docker push " + account_id + ".dkr.ecr.us-east-1.amazonaws.com/" + repository_name)
    print("done.")


if __name__ == "__main__":
    docker_file = "Dockerfile.preprocessing"
    aws_account_id = "176915357459"
    docker_image_name = "preprocessing"
    ecr_repository_name = "preprocessing"
    build(aws_account_id, docker_image_name, ecr_repository_name, docker_file)
