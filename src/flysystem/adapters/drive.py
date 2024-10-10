import io
import os
import pickle

from json import JSONDecodeError
from pickle import UnpicklingError
from typing import IO, Any, Dict, List, Optional

from google.auth.exceptions import TransportError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from tqdm import tqdm

from ..adapters import FilesystemAdapter
from ..error import (
    UnableToCreateDirectory,
    UnableToDownload,
    UnableToFindDriveToken,
    UnableToReadCredentialFile,
    UnableToUpload,
)

SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveFilesystemAdapter(FilesystemAdapter):
    """
    Google Drive filesystem adapter class
    """

    def __init__(self, creds_path: str, token_path: str = "/tmp/drive_token.pickle") -> None:
        self.creds = None
        # Check if file token.pickle exists
        if token_path and os.path.exists(token_path):
            # Read the token from the file and store it in the variable self.creds
            with open(token_path, "rb") as token:
                try:
                    self.creds = pickle.load(token)
                except UnpicklingError:
                    raise UnableToFindDriveToken.with_location(token_path, "Can't pickle the token.")

        # If no valid credentials are available, request the user to log in.
        if not self.creds or not self.creds.valid:
            # If token is expired, it will be refreshed, else, we will request a new one.
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                except JSONDecodeError:
                    raise UnableToReadCredentialFile.with_location(
                        creds_path, "The input credential file is not JSON formatted."
                    )
                self.creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle file for future usage
            with open(token_path, "wb") as token:
                pickle.dump(self.creds, token)

            # Connect to the API service
        self.service = build("drive", "v3", credentials=self.creds)

    def create_directory(self, path: str, options: Dict[str, Any] = None) -> Optional[str]:
        """
        Create a new directory.
        Arguments:
            path: The file name
            options: Options for create, include parent id
        Returns:
            String of directory id or None
        """
        file_metadata = {"name": path, "mimeType": "application/vnd.google-apps.folder"}

        if "parent_id" in options:
            file_metadata["parents"] = [options["parent_id"]]
        try:
            folder = self.service.files().create(body=file_metadata, fields="id").execute()
        except TransportError as e:
            raise UnableToCreateDirectory.with_location(path, str(e))
        except HttpError as err:
            raise UnableToCreateDirectory.with_location(path, err.reason)
        return folder.get("id")

    def upload_file(self, file_path: str, parent_id: str = None) -> Optional[str]:
        """
        Upload a file.
        Arguments:
            file_path: The file path
            parent_id: The parent id
        Returns:
            String of directory id or None
        """
        file_name = os.path.basename(file_path)
        file_metadata = {"name": file_name}
        if parent_id:
            file_metadata["parents"] = [parent_id]
        try:
            media = MediaFileUpload(file_path, resumable=True)
            file = self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        except TransportError as e:
            raise UnableToUpload.with_location(file_path, str(e))
        except HttpError as err:
            raise UnableToUpload.with_location(file_path, err.reason)
        except FileNotFoundError:
            raise UnableToUpload.with_location(file_path, "File not found.")
        except PermissionError as e:
            raise UnableToUpload.with_location(file_path, str(e))
        except Exception as e:
            raise UnableToUpload.with_location(file_path, str(e))
        return file.get("id")

    def upload_folder(self, folder_path: str, parent_id: str = None) -> None:
        """
        Upload a directory.
        Arguments:
            folder_path: The folder path
            parent_id: The parent id
        Returns:
            None
        """
        try:
            folder_name = os.path.basename(folder_path)
            folder_id = self.create_directory(folder_name, parent_id)
        except FileNotFoundError:
            raise UnableToUpload.with_location(folder_path, "Folder not found.")
        except PermissionError as e:
            raise UnableToUpload.with_location(folder_path, str(e))
        except Exception as e:
            raise UnableToUpload.with_location(folder_path, str(e))

        for item in tqdm(os.listdir(folder_path)):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path):
                self.upload_file(item_path, folder_id)
            elif os.path.isdir(item_path):
                self.upload_folder(item_path, folder_id)

    def get_resource_by_name(self, resource_name: str) -> Dict[str, Any]:
        try:
            query = f"name='{resource_name}' and trashed=false"
            response = (
                self.service.files()
                .list(q=query, fields="files(id, name, mimeType, createdTime, size, owners(displayName, emailAddress))")
                .execute()
            )

            files = response.get("files", [])
        except TransportError as e:
            raise UnableToDownload.with_location(resource_name, str(e))
        except HttpError as err:
            raise UnableToDownload.with_location(resource_name, err.reason)
        except Exception as e:
            raise UnableToDownload.with_location(resource_name, str(e))

        if not files:
            raise UnableToDownload.with_location(resource_name, "Resource not found")
        if len(files) == 1:
            return files[0]
        # Display detailed information for each file including owner
        print("Multiple files/folders found:")
        for idx, file in enumerate(files):
            size = file.get("size", "Unknown")
            owners = ", ".join(
                [f"{owner['displayName']} ({owner['emailAddress']})" for owner in file.get("owners", [])]
            )
            print(
                f"{idx + 1}. Name: {file['name']}, Type: {file['mimeType']}, "
                f"Created: {file['createdTime']}, Size: {size}, Owners: {owners}"
            )

        choice = int(input(f"Select a file or folder (1-{len(files)}): "))

        if 1 <= choice <= len(files):
            return files[choice - 1]
        else:
            raise UnableToDownload.with_location(resource_name, "Invalid choice")

    def download_file(self, file_id: str, file_name: str, download_path: str) -> None:
        """
        Download a file.
        Arguments:
            file_id: The file id
            file_name: The file name
            download_path: The download path
        Returns:
            None
        """
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

            fh.seek(0)
            os.makedirs(download_path, exist_ok=True)
            with open(os.path.join(download_path, file_name), "wb") as f:
                f.write(fh.read())
        except TransportError as e:
            raise UnableToDownload.with_location(file_name, str(e))
        except HttpError as err:
            raise UnableToDownload.with_location(file_name, err.reason)
        except Exception as e:
            raise UnableToDownload.with_location(file_name, str(e))
        print("File Downloaded")

    def download_folder(self, folder_id, folder_name: str, download_path: str) -> None:
        """
        Download a directory.
        Arguments:
            folder_id: The folder id
            folder_name: The folder name
            download_path: The download path
        Returns:
            None
        """
        download_path = os.path.join(download_path, folder_name)
        os.makedirs(download_path, exist_ok=True)

        try:
            results = (
                self.service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name, mimeType)").execute()
            )
        except TransportError as e:
            raise UnableToDownload.with_location(folder_name, str(e))
        except HttpError as err:
            raise UnableToDownload.with_location(folder_name, err.reason)
        except Exception as e:
            raise UnableToDownload.with_location(folder_name, str(e))
        items = results.get("files", [])

        for item in tqdm(items):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                new_folder_path = os.path.join(download_path, item["name"])
                os.makedirs(new_folder_path, exist_ok=True)
                self.download_folder(item["id"], item["name"], new_folder_path)
            else:
                self.download_file(item["id"], item["name"], download_path)
        print("Folder downloaded")

    def file_exists(self, path: str) -> bool:
        """
        Determine if a file exists.
        Arguments:
            path: The file path
        Returns:
            True if the file exsited
        """
        raise NotImplementedError

    def directory_exists(self, path: str) -> bool:
        """
        Determine if a directory exists.
        Arguments:
            path: The directory path
        Returns:
            True if the directory existed
        """
        raise NotImplementedError

    def write(self, path: str, contents: str, options: Dict[str, Any] = None):
        """
        Write the contents of a file.
        Arguments:
            path: The file path
            contents: The contents to write
            options: Write options
        Returns:
            None
        """
        raise NotImplementedError

    def write_stream(self, path: str, resource: IO, options: Dict[str, Any] = None):
        """
        Write the contents of a file from stream
        Arguments:
            path: The file path
            resource: The stream
            options: Write options
        Returns:
            None
        """
        raise NotImplementedError

    def read(self, path: str) -> str:
        """
        Get the contents of a file.
        Arguments:
            path: The file path
        Returns:
            The contents of file as string
        """
        raise NotImplementedError

    def read_stream(self, path: str) -> IO:
        """
        Read the contents of a file as stream
        Arguments:
            path: The file path
        Returns:
            The contents of file as stream
        """
        raise NotImplementedError

    def delete(self, path: str):
        """
        Delete a file
        Arguments:
            path: The file path
        Returns:
            None
        """
        raise NotImplementedError

    def delete_directory(self, path: str):
        """
        Recursively delete a directory.
        Arguments:
            path: Directory path to delete
        Returns:
            True if the directory is deleted successfully
        """
        raise NotImplementedError

    def set_visibility(self, path: str, visibility: str):
        """
        Set file visibility
        Arguments:
            path: The file path
            visibility: New visibility (Valid value: "public" and "private")
        Returns:
            None
        """
        raise NotImplementedError

    def visibility(self, path: str) -> str:
        """
        Get visibility of file
        Arguments:
            path: The file path
        Returns:
            The file's visibility
        """
        raise NotImplementedError

    def file_size(self, path: str) -> int:
        """
        Get size of file
        Arguments:
            path: The file path
        Returns:
            The file size in bytes
        """
        raise NotImplementedError

    def mime_type(self, path: str) -> str:
        """
        Get mimetype of file
        Arguments:
            path: The file path
        Returns:
            The file's mimetype
        """
        raise NotImplementedError

    def last_modified(self, path: str) -> int:
        """
        Get last modified time
        Arguments:
            path: The file path
        Returns:
            The file's last modified time as timestamp
        """
        raise NotImplementedError

    def list_contents(self, path: str) -> List[str]:
        """
        Get all (recursive) of the directories within a given directory.
        Arguments:
            path: Directory path
        Returns:
            List all directories in the given directory
        """
        raise NotImplementedError

    def copy(self, source: str, destination: str, options: Dict[str, Any] = None):
        """
        Copy a file
        Arguments:
            source: Path to source file
            destination: Path to destination file
            options: Copy options
        Returns:
            None
        """
        raise NotImplementedError

    def move(self, source: str, destination: str, options: Dict[str, Any] = None):
        """
        Move a file
        Arguments:
            source: Path to source file
            destination: Path to destination file
            options: Move options
        Returns:
            None
        """
        raise NotImplementedError

    def temporary_url(self, path: str, options: Dict[str, Any] = None):
        """
        Get pre-signed url of a file
        Arguments:
            path: The file path
            options: Temporary file options
        Returns:
            The pre-signed url of file as string
        """
        raise NotImplementedError
