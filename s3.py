import pickle
import fnmatch
import os
import boto3
import pprint
import re
import hashlib
from boto.s3.key import Key
from pyforms.basewidget import BaseWidget
from pyforms.controls import ControlFile
from pyforms.controls import ControlDir
from pyforms.controls import ControlText
from pyforms.controls import ControlSlider
from pyforms.controls import ControlPlayer
from pyforms.controls import ControlButton
from pyforms.controls import ControlPassword
from pyforms.controls import ControlTextArea
from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files
hiddenimports = ["pyforms.settings", "pyforms.gui.settings", "pyforms.controls"]
datas = collect_data_files('pyforms')


class S3Sync(BaseWidget):

    def __init__(self, *args, **kwargs):
        super().__init__('S3 Bucket Sync')

        # Definition of the forms fields
        self._putbutton = ControlButton('Upload')
        self._getbutton = ControlButton('Download')
        self._sourcedir = ControlDir('Source Directory')
        self._bucketname = ControlText('Bucket Name')
        self._bucketpath = ControlText('Bucket Path')
        self._username =  ControlText('Name')
        self._awsaccesskey = ControlText('AWS Access Key')
        self._awssecretkey = ControlPassword('AWS Secret')
        self._awssecretkey = ControlPassword('AWS Secret')
        self._messages = ControlTextArea('Status')
        self._messages.readonly = True
        self._local_file_list = []

        # Define the button actions
        self._putbutton.value = self.__putbuttonEvent

        # Load the data on startup
        self.__load()

        # Check MD5 of current files
        self._local_file_list = self.get_local_file_list(self._sourcedir.value)
        self._messages.value = str(self._local_file_list)
        message_buffer = ""
        for file in self._local_file_list:
            md5hash = self.get_md5(file)
            message_buffer += file+":"+md5hash+"\n"
        self._messages.value = message_buffer

        #get the current contents of the bucket
        self.get_s3_info()

        # Define the organization of the Form Controls
        self._formset = [
            '_username',
            '_sourcedir',
            '_bucketname',
            '_bucketpath',
            '_awsaccesskey',
            '_awssecretkey',
            '_messages',
            ('_putbutton',
             '_getbutton',)
        ]
    def get_s3_info(self):
      

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self._bucketname.value)
        #s3list = s3.list_objects_v2(Bucket=self._bucketname)
        s3list = bucket.load()

        print(s3list)
    
    def put_s3_files(self,file_list):
        s3 = boto3.resource('s3')
        client = boto3.client('s3')
        for file in file_list:
            filepath=file.split('/')
            filekey = self._bucketpath.value+"/"+filepath[-1]
            info = s3.meta.client.upload_file(file, self._bucketname.value,filekey)

            #Add tags to object
            response = client.put_object_tagging(
                Bucket=self._bucketname.value,
                Key=filekey,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'testkey',
                            'Value': 'testvalue'
                        },
                    ]
                }
            )
            print (response)
            
        


    def get_md5(self, filename):
        f = open(filename, 'rb')
        m = hashlib.md5()
        while True:
            data = f.read(10240)
            if len(data) == 0:
                break
            m.update(data)
        return m.hexdigest()

    def get_local_file_list(self, source_dir):
        files = []
        for root, dirnames, filenames in os.walk(source_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def compare_hash():
        files_to_upload = []
        for f in files:
            uri = to_uri(f)
        key = bucket.get_key(uri)
        if key is None:
            # new file, upload
            files_to_upload.append(f)
        else:
            # check MD5
            md5 = get_md5(f)
            etag = key.etag.strip('"').strip("'")
        if etag != md5:
            print(f + ": " + md5 + " != " + etag)
            files_to_upload.append(f)

    def __save(self):
        """
        Should serialize the object and save to a file
        """
        savevals = {'sourcedir': self._sourcedir.value,
                    'bucketpath': self._bucketpath.value,
                    'awsaccesskey': self._awsaccesskey.value,
                    'awssecretkey': self._awssecretkey.value,
                    'bucketname': self._bucketname.value,
                    'username': self._username.value,
                    }
        # Overwrites any existing file.
        with open(".savedata", 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)
        


        #push files into s3 bucket
        self.put_s3_files(self._local_file_list)

    def __load(self):
        config = Path('.savedata')
        if config.is_file():
            # Store configuration file value
            with open('.savedata', 'rb') as input:
                savevals = pickle.load(input)

            self._sourcedir.value = savevals['sourcedir']
            self._bucketpath.value = savevals['bucketpath']
            self._bucketname.value = savevals['bucketname']
            self._awsaccesskey.value = savevals['awsaccesskey']
            self._awssecretkey.value = savevals['awssecretkey']
            #self._username.value = savevals['username']

    def __putbuttonEvent(self):
        """
        After setting the best parameters run the full algorithm
        """
        #self._bucketname.value = "Upload not implimented"
        #self._awsaccesskey.value = "test2"
        #self._messages.value = "updated values"
        self.__save()
        self.repaint()
        # pass


if __name__ == '__main__':

    from pyforms import start_app
    start_app(S3Sync, geometry=(200, 200, 400, 400))
