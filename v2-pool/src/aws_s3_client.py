import boto3
import os
##############################################################################
#
#  S3 module
#
##############################################################################
s3client = boto3.client('s3')

def download_file(objectName, fileName):
   """
   download file
   :param objectName: the file path in S3
   :param fileName: the path and file name stored locally
   :return:
   """
   bucketname = os.environ['BUCKET_NAME']
   s3client.download_file(bucketname, objectName, fileName)

# return all objects using paging
def get_last_two_block_height_from_all_s3_folders_list(Prefix='output/'):
   folders_list = []
   paginator = s3client.get_paginator('list_objects_v2')
   bucketname = os.environ['BUCKET_NAME']
   pages = paginator.paginate( Bucket = bucketname, Delimiter = '/', Prefix = Prefix)
   for page in pages:
      CommonPrefixes = page.get("CommonPrefixes")
      for dir in CommonPrefixes:
        pathname = dir.get("Prefix")
        loc = pathname.find('_')+1
        blockheight = pathname[loc:len(pathname)-1]
        folders_list.append(int(blockheight))
   
   sorted_folders_list = sorted(folders_list)
   return (sorted_folders_list[-2], sorted_folders_list[-1])
   
def fetch_dcl_files_from_s3(block_height: int):
   file_list = ['/dcl_root.json','/dcl_pool.json','/dcl_user_liquidities.json','/dcl_user_orders.json','/dcl_pointinfo.json','/dcl_slotbitmap.json','/dcl_vip_users.json']
   for file in file_list:
      file_name = 'output/height_'+str(block_height)+file
      local_path = '.'+file
      download_file(file_name,local_path)
                
if __name__ == '__main__':
   (block_height1, block_height2) = get_last_two_block_height_from_all_s3_folders_list()
   #fetch_dcl_files_from_s3(91044865)
   fetch_dcl_files_from_s3(block_height2)