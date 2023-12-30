import json
import boto3
from boto3 import client as boto3_client
import os
import uuid
import base64
import pathlib
import datatier
import urllib.parse
import string

from configparser import ConfigParser

import PIL
from PIL import Image

lambda_client = boto3_client('lambda')

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: finalproject_compress**")
    
    # in case we get an exception, set this to a default
    # filename so we can write an error message if need be:
    local_results_file = "/tmp/compressed-image.jpg"
    bucketkey_results_file = ""
    
    # setup AWS based on config file:
    config_file = 'config.ini'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
    
    configur = ConfigParser()
    configur.read(config_file)
    
    # configure for S3 access:
    s3_profile = 's3readwrite'
    boto3.setup_default_session(profile_name=s3_profile)
    
    bucketname = configur.get('s3', 'bucket_name')
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    
    # configure for RDS access
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')
    
    # this function is event-driven by a PDF being
    # dropped into S3. The bucket key is sent to 
    # us and obtain as follows:
    bucketkey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    #prevent recursive calls
    if (len(bucketkey) >= 15 and bucketkey[-15:]=="-compressed.jpg"):
      return
    
    print("bucketkey:", bucketkey)
      
    extension = pathlib.Path(bucketkey).suffix
    
    if extension != ".jpg" and  extension != ".jpeg": 
      raise Exception("expecting S3 document to have .jpg extension")
    
    # bucketkey_results_file = bucketkey[0:-4] + ".jpg"
    if extension == ".jpg":
      bucketkey_results_file = bucketkey[0:-4] + "-compressed.jpg"
    elif extension == ".jpeg":
      bucketkey_results_file = bucketkey[0:-5] + "-compressed.jpeg"
    
    print("bucketkey results file:", bucketkey_results_file)
    print("local results file:", local_results_file)
      
    # download image from S3:
    print("**DOWNLOADING '", bucketkey, "'**")

    local_img = "/tmp/image.jpg"
    
    bucket.download_file(bucketkey, local_img)

    # compress image
    img = Image.open(local_img)
    width, height = img.size
    img = img.resize((width, height), Image.Resampling.LANCZOS)
    
    # save the results to local results file:
    img.save(local_results_file)


    # upload the results file to S3:
    print("**UPLOADING to S3 file", bucketkey_results_file, "**")

    bucket.upload_file(local_results_file,
                       bucketkey_results_file,
                       ExtraArgs={
                         'ACL': 'public-read',
                         'ContentType': 'text/plain'
                       })
    
    #invoke image recognition lambda function
    invoke_input = {'bucket': bucketname, 'bucketkey': bucketkey}
    invoke_response = lambda_client.invoke(FunctionName="arn:aws:lambda:us-east-2:003735305151:function:finalproj_rekog",
                                           InvocationType='Event',
                                           Payload=json.dumps(invoke_input))
                       
    
    
    # The last step is to update the database to change
    # the status of this job, and store the results
    # bucketkey:

    # TODO: open connection, and update job in database.
    # Change the status to 'completed', and update the
    # resultsfilekey.
    #
    # dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    # get the datafilekey?
    # sql = '''
    #       SELECT datafilekey
    #       FROM jobs
    #       WHERE condition;
    #       '''
    
    
    # sql = '''
    #       UPDATE jobs
    #       SET status = 'completed', resultsfilekey = %s
    #       WHERE datafilekey = %s;
    #       '''
    # datatier.perform_action(dbConn, sql, [bucketkey_results_file, bucketkey])



    # done!
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning success**")
    
    return {
      'statusCode': 200,
      'body': json.dumps("success")
    }
    

  #
  # on an error, try to upload error message to S3:
  #
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    
    # change
    # outfile = open(local_results_file, "w")

    # outfile.write(str(err))
    # outfile.write("\n")
    # outfile.close()
    
    if bucketkey_results_file == "": 
      # we can't upload the error file:
      pass
    else:
      # upload the error file to S3
      print("**UPLOADING**")
      bucket.upload_file(local_results_file,
                         bucketkey_results_file,
                         ExtraArgs={
                           'ACL': 'public-read',
                           'ContentType': 'text/plain'
                         })

    #
    # update jobs row in database:
    #
    #
    # TODO: open connection, update job in database
    # Change the status to 'error', and update the
    # resultsfilekey.
    #
    # dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    # sql = '''
    #       UPDATE jobs
    #       SET status = 'error', resultsfilekey = %s
    #       WHERE datafilekey = %s;
    #       '''
    # datatier.perform_action(dbConn, sql, [bucketkey_results_file, bucketkey])
    



    # done, return:
    return {
      'statusCode': 400,
      'body': json.dumps(str(err))
    }
