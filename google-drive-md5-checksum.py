from __future__ import print_function

import argparse
import os.path
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


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


def google_hash(folder_id, recursive):

    creds = google_credentials()

    service = build("drive", "v3", credentials=creds)
    directories = [{"id": folder_id, "name": "."}]

    for directorie in directories:
        for folders, files in google_scan_folder(service, directorie["id"]):
            print(f"\n{directorie['name']}:")
            directories += folders
            for file in files:
                print(file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Print files and their md5 checksum recursively."
    )
    parser.add_argument(
        "-f",
        "--folder",
        type=str,
        default=None,
        help="folder id where to start searching recursively",
    )
    parser.add_argument("-r", "--recursive", action="store_true")
    args = parser.parse_args()

    google_hash(args.folder, args.recursive)
