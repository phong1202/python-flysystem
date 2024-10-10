import argparse
import importlib
import os
import sys

import decouple

sys.path.append("../")


def get_argument():
    parser = argparse.ArgumentParser(description="Python Flysystem Uploader")

    parser.add_argument(
        "--adapter",
        help="The type of adapter",
        choices=["local", "s3", "drive", "huggingface", "memory"],
        required=True,
    )
    parser.add_argument("--upload-resource", help="The path of upload resource.", type=str, required=True)
    parser.add_argument("--env-file", "-e", type=str, help="Env file path.")

    # huggingface
    parser.add_argument("--repo-id", type=str, help="The id of huggingface repo.")
    parser.add_argument("--huggingface-token", type=str, help="The access token of Huggingface.")
    parser.add_argument("--message", type=str, help="The message of commit.")
    parser.add_argument("--path-in-repo", type=str, help="The path in repo.")
    parser.add_argument("--revision", type=str, help="The revision of commit.")

    # google drive
    parser.add_argument("--drive-creds-path", help="The path of Google Drive credentials json.", type=str)
    parser.add_argument(
        "--drive-token-path",
        help="The path of Google Drive token pickle file for quick access.",
        type=str,
        default="/tmp/drive_token.pickle",
    )
    parser.add_argument("--parent-id", type=str, help="The id of parent directory.")

    args = parser.parse_args()
    return args


def main():
    args = get_argument()
    if args.env_file:
        decouple.config = decouple.Config(decouple.RepositoryEnv(args.env_file))

    if args.adapter == "local":
        adapter_type = importlib.import_module("src.flysystem.adapters.local")

    elif args.adapter == "s3":
        adapter_type = importlib.import_module("src.flysystem.adapters.s3")

    elif args.adapter == "drive":
        adapter_type = importlib.import_module("src.flysystem.adapters.drive")
        adapter = adapter_type.DriveFilesystemAdapter(args.drive_creds_path, args.drive_token_path)

        if os.path.isdir(args.upload_resource):
            adapter.upload_folder(args.upload_resource, args.parent_id)
        else:
            adapter.upload_file(args.upload_resource, args.parent_id)

    elif args.adapter == "huggingface":
        adapter_type = importlib.import_module("src.flysystem.adapters.hgface")
        if args.huggingface_token:
            adapter = adapter_type.HuggingFaceFilesystemAdapter(args.huggingface_token)
        else:
            token = decouple.config("HUGGINGFACE_TOKEN")
            adapter = adapter_type.HuggingFaceFilesystemAdapter(token)
        adapter.upload(
            args.upload_resource,
            args.repo_id,
            args.message,
            args.path_in_repo,
            args.revision,
        )

    elif args.adapter == "memory":
        adapter_type = importlib.import_module("src.flysystem.adapters.memory")

    else:
        raise Exception("Invalid adapter")


if __name__ == "__main__":
    main()
