from __future__ import print_function

import argparse
import hashlib
import os.path
import pickle
import sys

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from progress.spinner import Spinner

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def google_credentials(creds=None):
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return creds


def google_scan_folder(service, folder_id=None):
    folders = list()
    files = list()

    query = f"'{folder_id}' in parents" if folder_id else None

    # Call the Drive v3 API
    request = service.files().list(
        pageSize=100,
        fields="nextPageToken, files(id, name, md5Checksum, mimeType)",
        q=query,
    )

    # Look inside the folder
    while request is not None:
        response = request.execute()

        for item in response.get("files", []):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                # If folder add to folder list
                folders.append(item)
            else:
                # If file, print
                files.append(item)

        yield folders, files

        request = service.files().list_next(request, response)


def google_hash(folder_id=None, recursive=False, verbose=False):
    files = list()
    files_without_hash = list()

    service = build("drive", "v3", credentials=google_credentials())
    folders = [{"id": folder_id, "name": "."}] if folder_id else list()

    spinner = Spinner("Scanning google-drive files: ")

    # Search
    for folder in folders:
        for _folders, _files in google_scan_folder(service, folder["id"]):
            folders += _folders

            if verbose:
                print(f"\n{folder['name']}:")

            for file in _files:
                if "md5Checksum" in file:
                    files.append(file["md5Checksum"])
                else:
                    files_without_hash.append(file["name"])

                if verbose:
                    print(file)
                else:
                    spinner.next()

    # Summary
    print(f"\n\tFound: {len(files)} files with checksum")
    if files_without_hash:
        print("Files without checksum")
        for file in files_without_hash:
            print(f"\n\t{file}")

    return files


def hash_file(path):
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    md5 = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()


def scan_local_files(path, recursive=False, verbose=False):

    local_files = dict()

    spinner = Spinner("Scanning local files: ")

    path = os.path.expanduser(path)
    for root, _, files in os.walk(path):

        if verbose:
            print(f"\n{os.path.join(root)}")

        for file in files:
            full_path = os.path.join(root, file)
            local_files[hash_file(full_path)] = {
                "name": file,
                "path": full_path,
            }

            if verbose:
                print(f"\t{file}")
            else:
                spinner.next()

    # Summary
    print(f"\n\tFound: {len(local_files)}")
    return local_files


def main(args):

    google_files_hashes = google_hash(
        args.google_folder_id, args.recursive, args.verbose
    )
    local_files = scan_local_files(args.local_folder)

    # Get duplicates
    dups = set(local_files.keys()).intersection(google_files_hashes)

    print("Already uploaded:")
    for dup in dups:
        print(f"\tFile: {local_files[dup]['name']} ", end="")
        if args.delete:
            print("deleting... ", end="")
            try:
                os.remove(local_files[dup]["path"])
            except Exception as e:
                print("error")
                print(e, end="")
            else:
                print("done", end="")
        print(end="\n")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Print files and their md5 checksum recursively."
    )
    parser.add_argument(
        "local_folder", type=str, help="folder id where to start searching recursively",
    )
    parser.add_argument(
        "google_folder_id",
        type=str,
        help="folder id where to start searching recursively",
    )
    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--delete", action="store_true")
    args = parser.parse_args()

    main(args)
