from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block")
# specify a subfolder of repo
github_block.get_directory("week_2/prefect-zoomcamp/flows/02_gcp")

if __name__ == "__main__":
    github_block.save("github-block", overwrite=True)
