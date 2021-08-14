import os

aws_account_id="176915357459"
docker_image_name="idealoctopotato"
ecr_repository_name="idealoctopotato"

print("starting docker build...")
os.system("docker build --pull --rm -f \"Dockerfile\" -t "+docker_image_name+":latest \".\"")
print("done.")

print("logging in aws ecr...")
os.system("aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "+aws_account_id+".dkr.ecr.us-east-1.amazonaws.com")
print("done.")

print("tagging...")
os.system("docker tag "+docker_image_name+":latest "+aws_account_id+".dkr.ecr.us-east-1.amazonaws.com/"+ecr_repository_name)
print("done.")

print("pushing to ecr...")
os.system("docker push "+aws_account_id+".dkr.ecr.us-east-1.amazonaws.com/"+ecr_repository_name)
print("done.")