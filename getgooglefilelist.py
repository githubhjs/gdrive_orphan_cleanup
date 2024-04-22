from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

import os
import pickle
import time

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

def create_orphan_folder(service):
    file_metadata = {
        'name': '_ORPHAN',
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    print(f'Created "_ORPHAN" folder with ID: {folder.get("id")}')
    return folder.get('id')

def authenticate_google_drive():
    """Authenticates the user with Google Drive and returns the service."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def get_orphan_files(service):
    """Get and remove orphan files in Google Drive."""
    page_token = None
    orphan_files = []
    attempt = 0
    total_files_processed = 0

    while True:  # Keep looping until no more orphan files/folders are found
        try:
            results = service.files().list(
                q="'me' in owners",
                pageSize=1000, fields="nextPageToken, files(id, name, parents, shared, mimeType)",
                pageToken=page_token).execute()

            items = results.get('files', [])

            if not items:
                print('No files found.')
                break
            else:
                for item in items:
                    total_files_processed += 1
                    # If the file has no parents and it's not shared, it is an orphan.
                    if not 'parents' in item and not item.get('shared', False):
                        # Move the file
                        service.files().update(fileId=item['id'], 
                                               addParents=orphan_folder_id,
                                               fields='id, parents').execute()
                                   
                        print(f'Moved orphan file/folder "{item["name"]}" to "_ORPHAN"') 

                page_token = results.get('nextPageToken', None)
                # If the request succeeded, reset the attempt count.
                attempt = 0

                print(f'Processed {total_files_processed} files so far. Found {len(orphan_files)} orphan files.')

        except HttpError as e:
            print('An HTTP error occurred:', e)
            attempt += 1
            if attempt <= 5:
                # Implementing exponential backoff.
                time.sleep(2**attempt)
                continue
            else:
                print('Too many errors occurred. Exiting.')
                break

    print('Finished processing files. Total files processed:', total_files_processed)

def main():
    """Main function."""
    service = authenticate_google_drive()
    get_orphan_files(service)

if __name__ == '__main__':
    main()
