import os
import requests
import base64
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
TOKEN = "YOUR_PERSONAL_ACCESS_TOKEN"
INSTANCE = "https://adb-984752964297111.11.azuredatabricks.net"
DBFS_DIRECTORY = "dbfs:/FileStore/hugues_demo"
WORKSPACE_DIRECTORY = "/hugues_demo/upload_big_jar"
LOCAL_FILE_PATH = "spark-core_2.12-3.4.0.jar"
DBFS_FILE_PATH = f"{DBFS_DIRECTORY}/spark-core_2.12-3.4.0.jar"
WORKSPACE_FILE_PATH = f"{WORKSPACE_DIRECTORY}/spark-core_2.12-3.4.0.jar"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks

def create_session():
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    })
    return session

def check_file_exists_in_dbfs(session, dbfs_path):
    url = f"{INSTANCE}/api/2.0/dbfs/get-status"
    data = {"path": dbfs_path.replace("dbfs:", "")}

    response = session.get(url, json=data)
    if response.status_code == 200:
        file_info = response.json()
        print(f"File exists in DBFS: {dbfs_path}")
        print(f"Size: {file_info.get('file_size', 0)/1024/1024:.2f} MB")
        return True, file_info.get('file_size', 0)
    return False, 0

def ensure_directory_exists_in_dbfs(session, dbfs_path):
    url = f"{INSTANCE}/api/2.0/dbfs/mkdirs"
    data = {"path": dbfs_path}

    response = session.post(url, json=data)
    if response.status_code == 200:
        print(f"Directory ensured in DBFS: {dbfs_path}")
    else:
        print(f"Error ensuring DBFS directory: {response.json()}")

def ensure_directory_exists_in_workspace(session, workspace_path):
    url = f"{INSTANCE}/api/2.0/workspace/mkdirs"
    data = {"path": workspace_path}

    response = session.post(url, json=data)
    if response.status_code == 200:
        print(f"Directory ensured in Workspace: {workspace_path}")
    else:
        print(f"Error ensuring Workspace directory: {response.json()}")

def upload_file_in_chunks(session, local_path, dbfs_path):
    file_size = os.path.getsize(local_path)
    print(f"Starting upload of {file_size/1024/1024:.2f} MB file to DBFS")

    create_url = f"{INSTANCE}/api/2.0/dbfs/create"
    create_data = {
        "path": dbfs_path,
        "overwrite": "true"
    }
    
    response = session.post(create_url, json=create_data)
    if response.status_code != 200:
        print(f"Error creating upload handle: {response.json()}")
        return False
    
    handle = response.json()['handle']
    add_block_url = f"{INSTANCE}/api/2.0/dbfs/add-block"
    uploaded = 0
    
    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
                
            chunk_b64 = base64.b64encode(chunk).decode()
            add_block_data = {
                "handle": handle,
                "data": chunk_b64
            }
            
            response = session.post(add_block_url, json=add_block_data)
            if response.status_code != 200:
                print(f"Error uploading chunk: {response.json()}")
                return False
                
            uploaded += len(chunk)
            progress = (uploaded / file_size) * 100
            print(f"Progress: {progress:.1f}% ({uploaded}/{file_size} bytes)")
    
    close_url = f"{INSTANCE}/api/2.0/dbfs/close"
    close_data = {"handle": handle}
    response = session.post(close_url, json=close_data)
    
    if response.status_code != 200:
        print(f"Error closing upload handle: {response.json()}")
        return False
        
    print(f"File uploaded successfully to DBFS at {dbfs_path}!")
    return True

def create_workspace_link(session, dbfs_path, workspace_path):
    """Create a link in the workspace that points to the DBFS file."""
    print(f"\nCreating workspace link...")
    
    url = f"{INSTANCE}/api/2.0/workspace/import"
    
    data = {
        "path": workspace_path,
        "format": "JAR",
        "language": "JAVA",
        "overwrite": True,
        "options": {
            "dbfs_path": dbfs_path
        }
    }
    
    try:
        response = session.post(url, json=data)
        if response.status_code == 200:
            print(f"Successfully created workspace link at {workspace_path}")
            return True
        else:
            print(f"Error creating workspace link: {response.text}")
            # Try alternative approach
            alt_data = {
                "path": workspace_path,
                "content": "",
                "object_type": "LIBRARY",
                "language": "SCALA",
                "overwrite": True,
                "format": "SOURCE"
            }
            alt_response = session.post(url, json=alt_data)
            if alt_response.status_code == 200:
                print(f"Successfully created workspace link using alternative method at {workspace_path}")
                return True
            else:
                print(f"Error creating workspace link with alternative method: {alt_response.text}")
                return False
    except Exception as e:
        print(f"Exception while creating workspace link: {str(e)}")
        return False

def reference_file_in_notebook(dbfs_path, file_size):
    print("\nFile is ready to use:")
    print(f"DBFS Path: {dbfs_path}")
    print(f"File size: {file_size/1024/1024:.2f} MB")
    print(f"""
To use this JAR:

1. In a notebook:
   spark.sparkContext.addJar("{dbfs_path}")

2. In a cluster:
   - Go to cluster configuration
   - Click on "Libraries"
   - Click "Install New"
   - Choose "DBFS" as the source
   - Enter the path: {dbfs_path}
""")

if __name__ == "__main__":
    # Create session with proper configuration
    session = create_session()
    
    # Step 1: Ensure directories exist
    ensure_directory_exists_in_dbfs(session, DBFS_DIRECTORY)
    ensure_directory_exists_in_workspace(session, WORKSPACE_DIRECTORY)

    # Step 2: Check if file exists in DBFS
    exists, file_size = check_file_exists_in_dbfs(session, DBFS_FILE_PATH)
    
    if exists:
        print("File already exists in DBFS, skipping upload.")
        success = True
    else:
        # Upload the file to DBFS if it doesn't exist
        success = upload_file_in_chunks(session, LOCAL_FILE_PATH, DBFS_FILE_PATH)
        if success:
            _, file_size = check_file_exists_in_dbfs(session, DBFS_FILE_PATH)

    # Step 3: Create a link in the workspace
    if success:
        create_workspace_link(session, DBFS_FILE_PATH, WORKSPACE_FILE_PATH)
        reference_file_in_notebook(DBFS_FILE_PATH, file_size)