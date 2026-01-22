import contextlib
from azure.storage.blob import BlobServiceClient
import os


class AzureWrapper(object):
    """Wrapper around azure (account, container) pair"""

    def __init__(self, account_url, sas_token, container_name):
        self.client = BlobServiceClient(account_url=account_url, credential=sas_token)
        self.container_name = container_name
        self.container_client = self.client.get_container_client(self.container_name)

    def get_blob_client(self, blob):
        return self.client.get_blob_client(container=self.container_name, blob=blob)

    def download_file(self, container_path, output_path):
        with contextlib.closing(self.get_blob_client(blob=container_path)) as blob_client:
            download_stream = blob_client.download_blob()

            with open(output_path, "wb") as fp:
                # perhaps we should download chunk-by-chunk, but this is fine for now
                fp.write(download_stream.readall())

    def upload_file(self, local_path, container_path):
        """Upload a local file to Azure Blob storage"""
        with contextlib.closing(self.get_blob_client(blob=container_path)) as blob_client:
            with open(local_path, "rb") as fp:
                blob_client.upload_blob(fp, overwrite=True)

    def upload_folder(self, local_folder, container_prefix="", show_progress=True):
        """Upload all files in a local folder to Azure Blob storage
        
        Args:
            local_folder: Path to local folder to upload
            container_prefix: Prefix path in container (e.g., "data/folder")
            show_progress: Whether to print progress messages
        
        Returns:
            Tuple of (success_count, total_count)
        """
        from pathlib import Path
        
        local_folder = Path(local_folder)
        if not local_folder.is_dir():
            raise ValueError(f"Local folder does not exist: {local_folder}")
        
        # Get all files recursively
        all_files = list(local_folder.rglob('*'))
        files_to_upload = [f for f in all_files if f.is_file()]
        
        success_count = 0
        total_count = len(files_to_upload)
        
        if show_progress:
            print(f"Found {total_count} files to upload from {local_folder}")
        
        for idx, file_path in enumerate(files_to_upload, 1):
            # Calculate relative path from local_folder
            relative_path = file_path.relative_to(local_folder)
            # Build container path
            if container_prefix:
                blob_path = f"{container_prefix}/{relative_path}".replace("\\", "/")
            else:
                blob_path = str(relative_path).replace("\\", "/")
            
            try:
                self.upload_file(str(file_path), blob_path)
                success_count += 1
                if show_progress:
                    print(f"[{idx}/{total_count}] ✓ {blob_path}")
            except Exception as e:
                if show_progress:
                    print(f"[{idx}/{total_count}] ✗ {blob_path}: {e}")
        
        return success_count, total_count

    def get_blob_exist_status(self, blob):
        return True if len(list(self.container_client.list_blobs(blob))) > 0 else False


def get_sas_token(name):
    token = os.getenv(name)
    if token is None:
        raise ValueError(f"The SAS token environment variable '{name}' is not set.")
    return token


def get_wxforecasting_azure_wrapper():
    return AzureWrapper(
        account_url="https://wxforecasting.blob.core.windows.net",
        sas_token=get_sas_token('wxforecasting_sas'),
        container_name="wxforecasting"
    )
