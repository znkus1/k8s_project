import os
from utils.minio import download_model, local_folder


if local_folder not in os.listdir():
    download_model()
