import boto3

def delete_metadata_folder(control_file):
    metadata_folder_to_delete = control_file.replace("doc_pdf", "exhibits").replace("control_files/", "").replace(".json", "")
    print(metadata_folder_to_delete)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('metadata-bucket-11')
    bucket.objects.filter(Prefix=metadata_folder_to_delete+"/").delete()


if __name__ == "__main__":
    control_file = "case_number/doc_pdf/control_files/folder1.json"
    # result should be like case_number/exhibits/folder1    
    delete_metadata_folder(control_file)