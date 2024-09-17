import os
import requests
from datetime import datetime, timedelta

DOCKERHUB_TOKEN = os.environ["DOCKERHUB_TOKEN"]
DELETE_AFTER_DAYS = os.environ["DELETE_AFTER_DAYS"]

def get_repo_tags(token):
    url = f"https://hub.docker.com/v2/repositories/scylladb/gocql-extended-ci/tags/"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get tags, Status Code: {response.status_code}, {response.text}")
        return []
    return response.json()["results"]

def delete_tag(tag, token):
    url = f"https://hub.docker.com/v2/repositories/scylladb/gocql-extended-ci/tags/{tag}/"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.delete(url, headers=headers)
    if response.status_code > 200 and response.status_code < 300:
        print(f"Deleted tag: {tag}")
        return True
    print(f"Failed to delete tag: {tag}, Status Code: {response.status_code}")
    return False

def clean_old_images():
    tags = get_repo_tags(DOCKERHUB_TOKEN)
    if tags is None:
        return False
    threshold_date = datetime.now() - timedelta(days=int(DELETE_AFTER_DAYS))
    status = True
    for tag in tags:
        last_pushed = datetime.strptime(tag["last_pushed"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if last_pushed < threshold_date:
            status = status and delete_tag(tag["name"], DOCKERHUB_TOKEN)
    return status

if __name__ == "__main__":
    if not clean_old_images():
        exit(1)
    exit(0)
