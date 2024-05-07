from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.ssh_exception import NoValidConnectionsError, SSHException
from kedro.io import AbstractDataSet
from typing import Dict, Any
import os
import errno
# import logging

# logger = logging.getLogger(__name__)

class SftpDataset(AbstractDataSet):
    def __init__(self, credentials: Dict[str, Any]):
        """
        :param hostname: Server host
        :param port: Server port
        :param username: Username for authentication
        :param password: Password for authentication
        """
        self.hostname = credentials['hostname']
        self.port = credentials['port']
        self.username = credentials['username']
        self.password = credentials['password']
        self.key_path = credentials['key_path']
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        self.sftp = None
        # print(f"SFTP Dataset initializing with\n{self.hostname}\n{self.port}\n{self.username}\n{self.key_path}\n")
        
    def _connect(self):
        try:
            # Load the private key
            key = RSAKey.from_private_key_file(self.key_path, password=self.password)
            
            # Connect using the private key
            self.ssh_client.connect(self.hostname, port=self.port, username=self.username, pkey=key)
            self.sftp = self.ssh_client.open_sftp()
            print("SSH connection established successfully.")
        except FileNotFoundError:
            print("Private key file not found.")
            raise
        except NoValidConnectionsError:
            print("Failed to connect to host on port: {0}".format(self.port))
            raise
        except SSHException as e:
            print("SSH session not established: {0}".format(e))
            raise
        except Exception as e:
            print("An error occurred: {0}".format(e))
            raise
    
    def get_current_directory(self):
        """ Returns the current working directory. """
        return self.sftp.getcwd()

    def change_directory(self, path):
        """ Changes the directory on the remote server. """
        self.sftp.chdir(path)

    def list_directory(self, path="."):
        """ Lists files and directories in the current or specified directory. """
        return self.sftp.listdir(path=path)

    def _close(self):
        """ Closes the SFTP connection. """
        if self.sftp is not None:
            self.sftp.close()
        self.ssh_client.close()
        
    def _save(self, file_mappings):
        self._connect()
        with self.ssh_client.open_sftp() as sftp:
            for local_path, remote_path in file_mappings.items():
                formatted_local_path = local_path.replace('\\', '/')
                
                # Check if the remote directory exists; if not, create it
                remote_dir = os.path.dirname(remote_path)
                
                # Ensure paths do not start with '/' if they are meant to be relative to the home directory
                if remote_dir.startswith('/'):
                    remote_dir = remote_dir[1:]
                if not self._mkdir_p(sftp, remote_dir):
                    # print(f"Failed to prepare directory: {remote_dir}")
                    continue  # Skip this file if we cannot prepare the directory

                try:
                    sftp.put(formatted_local_path, remote_path)
                    # print(f"Successfully uploaded: {formatted_local_path} to {remote_path}")
                except Exception as e:
                    print(f"Failed to upload {formatted_local_path} to {remote_path}: {e}")

    def _mkdir_p(self, sftp, remote_directory):
        """ Recursively create directory tree on the remote server if they don't exist. """
        current_dir = ''
        for dir_fragment in remote_directory.split('/'):
            if dir_fragment:  # Avoid empty strings, which can occur due to leading slashes
                if current_dir:
                    current_dir += '/' + dir_fragment  # Append to build the full path
                else:
                    current_dir = dir_fragment  # Start with the first valid fragment
                
                try:
                    attrs = sftp.stat(current_dir)
                    # print(f"Directory exists. Size: {attrs.st_size} Permissions: {attrs.st_mode}")
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        # Directory does not exist, so try to create it
                        try:
                            sftp.mkdir(current_dir)
                            # print(f"Created directory: {current_dir}")
                        except IOError as e:
                            print(f"Failed to create directory {current_dir}: {e}")
                            return False
                    else:
                        # Handle other I/O errors differently if necessary
                        print(f"Error accessing directory {current_dir}: {e}")
                        return False
        return True
    
    # def execute_command(self, command):
    #     """Executes a shell command on the remote server."""
    #     # self._connect()  # Ensure connection is open
    #     stdin, stdout, stderr = self.ssh_client.exec_command(command)
    #     output = stdout.read().decode()
    #     error = stderr.read().decode()
    #     if error:
    #         raise Exception(f"Error executing command: {error}")
    #     return output
    
    def _load(self):
        print(f"SFTP Dataset does not currently support extracting files from remote server.")
        pass

    def _exists(self):
        # Optionally implement this if needed to check if a file exists at one of the remote_paths
        pass

    def _describe(self):
        return {
            "host": self.hostname,
            "port": self.port,
            "username": self.username
        }
