import os
import requests
from datetime import datetime, timedelta

DOCKERHUB_USERNAME = os.environ["DOCKERHUB_USERNAME"]
DOCKERHUB_TOKEN = os.environ["DOCKERHUB_TOKEN"]
DELETE_AFTER_DAYS = os.environ["DELETE_AFTER_DAYS"]

def get_docker_token(username, password):
    url = "https://hub.docker.com/v2/users/login/"
    headers = {"Content-Type": "application/json"}
    data = {"username": username, "password": password}

    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        return response.json()["token"]
    else:
        print(f"Failed to login to DockerHub: {response.status_code}")
        return None

def get_repo_tags(token):
    url = f"https://hub.docker.com/v2/repositories/scylladb/gocql-extended-ci/tags/"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get tags, Status Code: {response.status_code}, {response.text}")
        return None
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
    token = get_docker_token(DOCKERHUB_USERNAME, DOCKERHUB_TOKEN)
    if token is None:
        return False
    tags = get_repo_tags(token)
    if tags is None:
        return False
    threshold_date = datetime.now() - timedelta(days=int(DELETE_AFTER_DAYS))
    status = True
    for tag in tags:
        last_updated = datetime.strptime(tag["last_updated"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if last_updated < threshold_date:
            status = status and delete_tag(tag["name"], token)
    return status

if __name__ == "__main__":
    if not clean_old_images():
        exit(1)
    exit(0)
