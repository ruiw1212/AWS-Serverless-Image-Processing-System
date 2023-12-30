# Python program to open and process a JPG file,
# and save the results to a text file. 

import json
import boto3
import os
import uuid
import base64
import pathlib
import datatier
import urllib.parse
import string

from configparser import ConfigParser
from pypdf import PdfReader
from PIL import Image
from PIL.ExifTags import TAGS


def extract_pdf_metadata(pdf_path):
    """
    Extract metadata from a PDF file.
    
    :param pdf_path: Path to the PDF file
    :return: Dictionary containing metadata, or None if metadata is not available
    """
    try:
        reader = PdfReader(pdf_path)
        metadata = reader.metadata

        # Metadata is usually a dictionary-like object
        if metadata is not None:
            # Convert to a standard dictionary and return
            return {key[1:]: value for key, value in metadata.items()}  # Remove the '/' from keys
        else:
            return None
    except Exception as e:
        print("**ERROR**")
        print(f"Error reading PDF metadata: {e}")
        return None
        
        
def extract_jpg_metadata(jpg_path):
    """
    Extract metadata from a JPEG file.
    
    :param jpeg_path: Path to the JPEG file
    :return: Dictionary containing metadata, or None if metadata is not available
    """
    try:
        # Open the image file
        
        with Image.open(jpg_path) as img:
            print(f"Image opened successfully: {img.format}, {img.size}")
            # extract other basic metadata
            info_dict = {
                "Image Size": img.size,
                "Image Height": img.height,
                "Image Width": img.width,
                "Image Format": img.format,
                "Image Mode": img.mode,
                "Image is Animated": getattr(img, "is_animated", False),
                "Frames in Image": getattr(img, "n_frames", 1)
            }
            
            for label,value in info_dict.items():
              print(f"{label:25}: {value}")
              # Get EXIF data
              exif_data = img._getexif()
              # print("exif_data",exif_data)

            # Proceed only if EXIF data is found
            if exif_data is not None:
                # Decode EXIF data
                exif = {
                    TAGS.get(key): value
                    for key, value in exif_data.items()
                    if key in ExifTags.TAGS
                }
                # Combine basic metadata and EXIF data
                info_dict.update(exif)
            return info_dict
            
    except Exception as e:
          print("**ERROR**")
          print(f"Error type: {type(e).__name__}")
          print(f"Error reading JPEG metadata: {e}")
          return None

        
def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: final_proj_metdata**")
    
    #
    # in case we get an exception, set this to a default
    # filename so we can write an error message if need
    # be:
    local_results_file = "/tmp/results.txt"
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
    # bucket = event[bucket]
    
    
    # configure for RDS access
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')
    
    # this function is event-driven by a PDF being
    # dropped into S3. The bucket key is sent to 
    # us and obtain as follows:
    # bucketkey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    bucketkey = event["bucketkey"]
    
    print("bucketkey:", bucketkey)
      
    extension = pathlib.Path(bucketkey).suffix
    
    
    if extension.lower() != ".jpeg" and extension.lower() != ".jpg" : 
      raise Exception("expecting S3 document to have .jpeg extension")
    
    bucketkey_results_file = bucketkey[0:-4] + "-metadata.txt"
    

    
    print("bucketkey results file:", bucketkey_results_file)
    print("local results file:", local_results_file)
      
    #
    # download jpeg from S3:
    #
    print("**DOWNLOADING '", bucketkey, "'**")

    local_jpg = "/tmp/data.jpg"
    
    bucket.download_file(bucketkey, local_jpg)

    #
    # open jpg file:
    #
    print("**PROCESSING local JPG**")
    
    # reader = PdfReader(local_pdf)
    # number_of_pages = len(reader.pages)
    

    # if os.path.exists(local_jpg):
    #     print(f"File {local_jpg} exists.")
    # else:
    #     print(f"File {local_jpg} does not exist.")
        
    # size = os.path.getsize(local_jpg)
    # print(f"File size: {size} bytes")
    # with open(local_jpg, 'rb') as file:
    #   content = file.read(10)
    #   print(content)  # This will print the first 10 bytes of the file  


    # Usage example
    jpg_metadata = extract_jpg_metadata("/tmp/data.jpg")
    if jpg_metadata:
      print("JPG Metadata:", jpg_metadata)
    else:
      print("No metadata found or error occurred.")
      
    
    # write the results to local results file:
    outfile = open(local_results_file, "w")
    outfile.write("**METADATA**\n")
    outfile.write(str(jpg_metadata))
    outfile.close()
    
    
    # upload the results file to S3:
    print("**UPLOADING to S3 file", bucketkey_results_file, "**")

    bucket.upload_file(local_results_file,
                       bucketkey_results_file,
                       ExtraArgs={
                         'ACL': 'public-read',
                         'ContentType': 'text/plain'
                       })
    
    # 
    # The last step is to update the database to change
    # the status of this job, and store the results
    # bucketkey:

    #
    # TODO: open connection, and update job in database.
    # Change the status to 'completed', and update the
    # resultsfilekey.
    #
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    sql = """UPDATE jobs SET status = 'completed', resultsfilekey = %s WHERE datafilekey = %s"""
    datatier.perform_action(dbConn, sql, [bucketkey[0:-4] + "-compressed.jpg", bucketkey])

    #
    # done!
    #
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
    
    outfile = open(local_results_file, "w")

    outfile.write(str(err))
    outfile.write("\n")
    outfile.close()
    
    if bucketkey_results_file == "": 
      #
      # we can't upload the error file:
      #
      pass
    else:
      # 
      # upload the error file to S3
      #
      print("**UPLOADING**")
      #
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
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    sql = """UPDATE jobs SET status = 'error', resultsfilekey = %s WHERE datafilekey = %s"""
    datatier.perform_action(dbConn, sql, [bucketkey[0:-4] + "-compressed.jpg", bucketkey])


    #
    # done, return:
    #    
    return {
      'statusCode': 400,
      'body': json.dumps(str(err))
    }