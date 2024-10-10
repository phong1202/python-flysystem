import argparse
import importlib
import sys

import decouple

sys.path.append("../")


def get_argument():
    parser = argparse.ArgumentParser(description="Python Flysystem Downloader")

    parser.add_argument(
        "--adapter",
        help="The type of adapter",
        choices=["local", "s3", "drive", "huggingface", "memory"],
        required=True,
    )
    parser.add_argument("--download-path", help="The path of download resource.", type=str, required=True)
    parser.add_argument("--resource", type=str, help="Name or path of resource need download.")
    parser.add_argument("--env-file", "-e", type=str, help="Env file path.")

    # huggingface
    parser.add_argument("--repo-id", type=str, help="The id of huggingface repo.")
    parser.add_argument("--huggingface-token", type=str, help="The access token of Huggingface.")

    # google drive
    parser.add_argument("--drive-creds-path", help="The path of Google Drive credentials json.", type=str)
    parser.add_argument(
        "--drive-token-path",
        help="The path of Google Drive token pickle file for quick access.",
        type=str,
        default="/tmp/drive_token.pickle",
    )

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

        if not args.resource:
            raise Exception("Resource name is required.")
        resource = adapter.get_resource_by_name(args.resource)
        print(resource)
        if resource.get("mimeType") == "application/vnd.google-apps.folder":
            adapter.download_folder(resource["id"], resource["name"], args.download_path)
        else:
            adapter.download_file(resource["id"], resource["name"], args.download_path)

    elif args.adapter == "huggingface":
        adapter_type = importlib.import_module("src.flysystem.adapters.hgface")
        if args.huggingface_token:
            adapter = adapter_type.HuggingFaceFilesystemAdapter(args.huggingface_token)
        else:
            token = decouple.config("HUGGINGFACE_TOKEN")
            adapter = adapter_type.HuggingFaceFilesystemAdapter(token)
        adapter.download(args.repo_id, args.download_path, args.resource)

    elif args.adapter == "memory":
        adapter_type = importlib.import_module("src.flysystem.adapters.memory")

    else:
        raise Exception("Invalid adapter")


if __name__ == "__main__":
    main()
