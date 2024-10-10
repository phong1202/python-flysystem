import os.path
import shutil

from typing import IO, Any, Dict, List, Optional

from huggingface_hub import HfApi, snapshot_download
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError, RevisionNotFoundError

from src.flysystem.adapters import FilesystemAdapter
from src.flysystem.error import UnableToDownload, UnableToUpload


class HuggingFaceFilesystemAdapter(FilesystemAdapter):
    """
    Hugging Face Filesystem Adapter
    """

    def __init__(self, token: str) -> None:
        self.token = token
        self.api = HfApi(token=token)

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

    def create_directory(self, path: str, options: Dict[str, Any] = None):
        """
        Create a directory.
        Arguments:
            path: Directory path to create
            options: Options for create
        Returns:
            True if the directory is created successfully
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

    def download(self, repo_id: str, local_dir: str, resource: Optional[str] = None):
        """
        Download resource from Hugging Face
        Arguments:
            repo_id: The repository id
            local_dir: The path that resource saved after downloading.
            resource: File or folder name in repo need downloading. If not resource, download all repo.
        Returns:
            None
        """
        try:
            if not resource:
                snapshot_download(repo_id=repo_id, local_dir=local_dir, token=self.token)
            else:
                file_path_list = self.api.list_repo_files(repo_id=repo_id)
                for file_path in file_path_list:
                    if file_path.startswith(resource):
                        tmp_path = os.path.join(local_dir, "tmp")
                        self.api.hf_hub_download(repo_id=repo_id, filename=file_path, local_dir=tmp_path)
                        src_path = os.path.join(tmp_path, resource)
                        dst_path = os.path.join(local_dir, resource.split("/")[-1])
                        shutil.move(src_path, dst_path)
                        shutil.rmtree(tmp_path)
        except RepositoryNotFoundError:
            raise UnableToDownload.with_location(repo_id, "Repository not found.")
        except RevisionNotFoundError:
            raise UnableToDownload.with_location(repo_id, "Revision not found.")
        except HfHubHTTPError as e:
            raise UnableToDownload.with_location(repo_id, str(e))
        except ValueError:
            raise UnableToDownload.with_location(repo_id, "Invalid arguments.")
        except Exception as e:
            raise UnableToDownload.with_location(repo_id, str(e))
        print(f"Resource downloaded to {local_dir}")

    def upload(
        self,
        local_resource_path: str,
        repo_id: str,
        commit_message: str,
        path_in_repo: Optional[str],
        revision: Optional[str],
    ):
        """
        Upload resource folder to Hugging Face
        Arguments:
            local_resource_path: The local path of upload resource.
            repo_id: The repository id.
            commit_message: The commit message to the repository.
            path_in_repo: Relative path in the repository. If null, it will be the file name or folder name
            revision: Revision to commit from. If null, it will be "main" branch
        Returns:
            None
        """
        print(f"Uploading resource to {repo_id}...")
        try:
            if not path_in_repo:
                path_in_repo = local_resource_path.split("/")[-1]
            if os.path.isdir(local_resource_path):
                self.api.upload_folder(
                    folder_path=local_resource_path,
                    path_in_repo=path_in_repo,
                    repo_id=repo_id,
                    commit_message=commit_message,
                    revision=revision,
                )
            else:
                self.api.upload_file(
                    path_or_fileobj=local_resource_path,
                    path_in_repo=path_in_repo,
                    repo_id=repo_id,
                    commit_message=commit_message,
                    revision=revision,
                )
        except RepositoryNotFoundError:
            raise UnableToUpload.with_location(repo_id, "Repository not found.")
        except RevisionNotFoundError:
            raise UnableToUpload.with_location(repo_id, "Revision not found.")
        except HfHubHTTPError as e:
            raise UnableToUpload.with_location(repo_id, str(e))
        except ValueError:
            raise UnableToUpload.with_location(repo_id, "Invalid arguments.")
        except Exception as e:
            raise UnableToUpload.with_location(repo_id, str(e))
        print(f"Resource uploaded to {repo_id}")
