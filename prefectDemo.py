from datetime import timedelta
from prefect import flow,task
from git import Repo
import requests, os

LOCAL_FOLDER = r"E:\Prefect"  # Using raw string (r"") for Windows paths
GITHUB_USERNAME = "Farhan-Uz-Zaman"
REPO_NAME = "Prefect"
COMMIT_MESSAGE = "Automated commit from Prefect"

# === Pull token from environment variable ===
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise EnvironmentError("GITHUB_TOKEN not found.")

REMOTE_URL = f"https://{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{REPO_NAME}.git"


#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
@task
def response(url):
    data = requests.get(url)
    status = data.json()
    clean_data = cleaning(status)
    print(clean_data)

@task
def cleaning(status):
    clean_data = status
    return clean_data


@task
def init_git_repo(path: str):
    if not os.path.isdir(os.path.join(path, ".git")):
        repo = Repo.init(path)
        print("✅ Initialized new Git repo.")
    else:
        repo = Repo(path)
        print("✅ Using existing Git repo.")
    return repo

@task
def commit_and_push(repo: Repo, message: str, remote_url: str):
    repo.git.add(A=True)
    if repo.is_dirty(untracked_files=True):
        repo.index.commit(message)
        print("✅ Changes committed.")
    else:
        print("ℹ️ No changes to commit.")

    if "origin" not in [r.name for r in repo.remotes]:
        repo.create_remote("origin", remote_url)
        print("✅ Added remote 'origin'.")

    try:
        # Create a new branch to avoid protected master/main
        new_branch = "automated-push"
        if new_branch not in repo.heads:
            repo.git.checkout("-b", new_branch)
        else:
            repo.git.checkout(new_branch)

        repo.git.push("--set-upstream", "origin", new_branch, force=True)
        print(f"🚀 Pushed to new branch '{new_branch}'!")
    except Exception as e:
        print(f"❌ Push failed: {e}")
        
@flow(name="GitHub Push Flow")
def git_push_flow():
    repo = init_git_repo(LOCAL_FOLDER)
    commit_and_push(repo, COMMIT_MESSAGE, REMOTE_URL)

@flow
def collect_petstore_data():
    url= "https://petstore.swagger.io/v2/store/inventory"
    get_response = response(url)
    

def main():
    collect_petstore_data()
if __name__ == "__main__":
    main()
    git_push_flow()
   
#@task
#def insert_to_db(inventory, db_host, db_user,db_name):
    



   
