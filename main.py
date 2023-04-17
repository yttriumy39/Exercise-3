import boto3
import os, gzip

def main():
    # your code here

    bucket = "commoncrawl"
    key_1 = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    #create directory if does not exist
    cwd = os.getcwd()
    directory =  cwd + "/data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    #exercise uses boto3 to dowload data from s3. Would be preferable to hold credentials in external credentials file
    s3= boto3.client('s3',
    aws_access_key_id="access_key",
    aws_secret_access_key="secret_access_key",
    )

    #download from s3
    s3.download_file(bucket, key_1, directory + "/" + "datafile.gz")

    #get uri for downloading file 2
    file1 = gzip.open(cwd + '/data/datafile.gz','r')
    key_2 = file1.readline().decode("utf-8")
    key_2 = key_2.strip()

    #download file 2 and print lines
    s3.download_file(bucket, key_2, directory + "/" + "datafile2.gz")
    with gzip.open(cwd + '/data/datafile2.gz','r') as f:        
        for line in f:        
            print(line)
    pass


if __name__ == '__main__':
    main()
