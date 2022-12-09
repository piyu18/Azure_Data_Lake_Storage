#Import libraries
import paramiko
import time
from datetime import datetime, timedelta
import os

class SftpClient:

    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

        self.create_connection(self.host,self.username, self.password)
        self.create_directory()

    def create_connection(self, host, username, password):
        '''
            Function will establish connection with
            SFTP.

            Parameters:
            --------------
            host: str
                Provide hostname
            
            username: str
                Provide the user name for sftp connection
            
            password: str
        '''
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=username, password=password)
        print("Connected with hostname = ",host, ", username = ", username)
        return ssh

    def create_directory(self):
        '''
            Function will create directory
            with today's date
        '''
        today = datetime.now()
        try:
            if not os.path.isdir(today.strftime('%Y%m%d')):
                os.mkdir(today.strftime('%Y%m%d'))
        except Exception as e:
            print(e)

    def download_files(self, remote_path, max_files=10,date1=datetime(2022, 12, 8, 11, 59, 45)):
        '''
        Function will download files to local,
        in this case it will download files in a
        directory with today's date

        Parameters:
        --------------
        remote_path: str

        max_files: int, default value is 10
            give the value according to no of files you
            want to download
        
        date1: datetime
        '''
        count = 0
        max_files = max_files
        file_path = datetime.now().strftime('%Y%m%d')+'/'
        sftp = self.create_connection(host, username, password).open_sftp()
        for f in sorted(sftp.listdir_attr(remote_path), key=lambda k: (datetime.fromtimestamp(k.st_mtime)>date1), reverse=True):
        #print(f.filename)
            count+=1 
            if count > max_files:
                break  
            if f.filename.endswith('.wav'):
                if f.filename not in file_path:
                    sftp.get(f.filename,file_path+f.filename)
        

if __name__ == '__main__':
    host = 'hostname'
    username = 'username'
    password = 'password'

    remote_path = 'remotepath'


    client = SftpClient(host,username, password)
    client.download_files(remote_path)