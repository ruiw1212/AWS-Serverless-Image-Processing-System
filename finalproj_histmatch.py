import json
import boto3
import os
import numpy as np
import base64
import datatier
import cv2
from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: hist_match**")
    #
    # setup AWS based on config file:
    #
    config_file = 'config.ini'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
    configur = ConfigParser()
    configur.read(config_file)
    #
    # configure for S3 access:
    #
    s3_profile = 's3readonly'
    boto3.setup_default_session(profile_name=s3_profile)
    bucketname = configur.get('s3', 'bucket_name')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    #
    # configure for RDS access
    #
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')
    #
    # jobid from event: could be a parameter
    # or could be part of URL path ("pathParameters"):
    #
    print(event["pathParameters"])
    if "source" in event:
      source = event["source"]
    elif "pathParameters" in event:
      if "source" in event["pathParameters"]:
        source = event["pathParameters"]["source"]
      else:
        raise Exception("requires source parameter in pathParameters")
    else:
        raise Exception("requires source parameter in event")
    if "target" in event:
      target = event["target"]
    elif "pathParameters" in event:
      if "target" in event["pathParameters"]:
        target = event["pathParameters"]["target"]
      else:
        raise Exception("requires target parameter in pathParameters")
    else:
        raise Exception("requires target parameter in event")
    #
    # does the jobid exist?  What's the status of the job if so?
    #
    # open connection to the database:
    #
    print("**Opening connection**")
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    #
    # first we need to make sure the userid is valid:
    #
    print("**Checking if source is valid**")
    sql = "select * from jobs where jobid = %s;"
    row = datatier.retrieve_one_row(dbConn, sql, [source])
    if row == ():  # no such job
      print("**No such source, returning...**")
      return {
        'statusCode': 400,
        'body': json.dumps("no such source image...")
      }
    status = row[2]
    origin_source_name = row[3]
    source_key = row[5]
    #
    # what's the status of the job?
    #
    if status == "pending":
      print("**Source still pending, returning...**")
      #
      return {
        'statusCode': 400,
        'body': json.dumps("pending")
      }
    if status == 'error':
      #
      # let's download the results if available, and return the
      # error message in the results file:
      #
      if results_file_key == "":
        print("**Source not upload sucessfully, returning...**")
        #
        return {
          'statusCode': 400,
          'body': json.dumps("ERROR: unknown")
        }
    print("**Checking if target is valid**")
    sql = "select * from jobs where jobid = %s;"
    row = datatier.retrieve_one_row(dbConn, sql, [target])
    if row == ():  # no such job
      print("**No such target, returning...**")
      return {
        'statusCode': 400,
        'body': json.dumps("no such target image...")
      }
    status = row[2]
    origin_target_name = row[3]
    target_key = row[5]
    #
    # what's the status of the job?
    #
    if status == "pending":
      print("**Target still pending, returning...**")
      #
      return {
        'statusCode': 400,
        'body': json.dumps("pending")
      }
    if status == 'error':
      #
      # let's download the results if available, and return the
      # error message in the results file:
      #
      if results_file_key == "":
        print("**Target not upload sucessfully, returning...**")
        #
        return {
          'statusCode': 400,
          'body': json.dumps("ERROR: unknown")
        }
    #
    # if we get here, the job completed. So we should have results
    # to download and return to the user:
    #
    local_source_filename = "/tmp/source.png"
    local_target_filename = "/tmp/target.png"
    local_result_filename = "/tmp/result.png"
    print("**Downloading results from S3**")
    bucket.download_file(source_key, local_source_filename)
    bucket.download_file(target_key, local_target_filename)
    print("**Conducting image matching**")
    source_im = cv2.resize(cv2.imread(local_source_filename), (128, 128))
    target_im = cv2.resize(cv2.imread(local_target_filename), (128, 128))
    H1,W1,C1 = source_im.shape
    H2,W2,C2 =   target_im.shape
    out = np.hstack([source_im, cv2.resize(target_im, (W1,H1))])
    for i in range(C1):
      source_hist, _ = np.histogram(source_im[:, :, i:i+1], 256, (0, 256))
      source_hist = source_hist.cumsum() / (H1 * W1)
      target_hist, _ = np.histogram(target_im[:, :, i:i+1], 256, (0, 256))
      target_hist = target_hist.cumsum() / (H2 * W2)
      mapping = np.interp(source_hist, target_hist, np.arange(256))
      source_im[:, :, i:i+1] = mapping[source_im[:, :, i:i+1]]
    out = np.hstack([out, source_im])
    retval, buffer_img= cv2.imencode('.jpg', out)
    data = base64.b64encode(buffer_img)
    datastr = data.decode()
    print("**DONE, returning results**")
    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    res_body = {'data': datastr, 'source': origin_source_name, 'target': origin_target_name}
    return {
      'statusCode': 200,
      # 'body': json.dumps(datastr),
      'body': json.dumps(res_body),
      # 'source' : json.dumps(str(origin_source_name)),
      # 'target' : json.dumps(str(origin_target_name))
    }
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    return {
      'statusCode': 400,
      'body': json.dumps(str(err))
    }