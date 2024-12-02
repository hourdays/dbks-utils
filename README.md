# Utilities List
- [upload_big_jar.py](https://github.com/hourdays/dbks-utils/blob/main/upload_big_jar.py) allows you to upload files bigger than 10 MB to DBFS and then puts them in a Workspace folder. It needs to be improved because it creates a notebook inside the workspace folder instead of a file but a file cannot be created so we might as well create a notebook with useful code inside to load the jar from the DBFS directory.
- list_dbks_workspace_notebooks_batch_resume.py and list_dbks_workspace_notebooks_batch_resume_multithreaded.py
