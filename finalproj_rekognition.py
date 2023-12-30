import boto3
from boto3 import client as boto3_client
import logging
from botocore.exceptions import ClientError
import json
import os

# Instantiate logger
logger = logging.getLogger(__name__)

# Connect to the Rekognition client and S3 client
rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')


lambda_client = boto3_client('lambda')

def lambda_handler(event, context):
    try:
        print("hello1")
        # Extract S3 bucket and object key from the event
        # s3_bucket = event['Records'][0]['s3']['bucket']['name']
        # s3_object_key = event['Records'][0]['s3']['object']['key']
        s3_bucket = event["bucket"]
        s3_object_key = event["bucketkey"]
        
        # Extract the image name without the file extension
        # image_name = os.path.splitext(os.path.basename(s3_object_key))[0]
        image_name = s3_object_key[0:]

        # Retrieve the image content from S3
        response = s3.get_object(Bucket=s3_bucket, Key=s3_object_key)
        image = response['Body'].read()

        # Analyze the image using Amazon Rekognition
        response = rekognition.detect_labels(Image={'Bytes': image})
        labels = [label['Name'] for label in response['Labels']]
        print("Labels found:")
        print(labels)

        # Create a .txt file with labels
        labels_txt = '\n'.join(labels)
        labels_filename = f"{image_name[0:-4]}-labels.txt"
        s3.put_object(Body=labels_txt, Bucket=s3_bucket, Key=labels_filename)
        
        
        #invoke metadata lambda function
        invoke_input = {'bucket': s3_bucket, 'bucketkey': s3_object_key}
        invoke_response = lambda_client.invoke(FunctionName="arn:aws:lambda:us-east-2:003735305151:function:finalproj_metadata",
                                           InvocationType='Event',
                                           Payload=json.dumps(invoke_input))

        # Construct a response with the filename
        lambda_response = {
            "statusCode": 200,
            "body": json.dumps({"LabelFileName": labels_filename})
        }

    except ClientError as client_err:
        error_message = "Couldn't analyze image: " + client_err.response['Error']['Message']
        lambda_response = {
            'statusCode': 400,
            'body': {
                "Error": client_err.response['Error']['Code'],
                "ErrorMessage": error_message
            }
        }
        logger.error("Error function %s: %s",
                     context.invoked_function_arn, error_message)

    except Exception as e:
        lambda_response = {
            'statusCode': 400,
            'body': {
                "Error": "Error",
                "ErrorMessage": str(e)
            }
        }
        logger.error("Error function %s: %s",
                     context.invoked_function_arn, str(e))

    return lambda_response