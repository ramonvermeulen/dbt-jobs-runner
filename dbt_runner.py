import json
import logging
import time
from urllib.request import Request, urlopen


class DbtCloudRunner:
    """
    Helper class to run dbt jobs in dbt cloud using the dbt API version 2
    """

    run_status_map = {
        1: "Queued",
        2: "Starting",
        3: "Running",
        10: "Success",
        20: "Error",
        30: "Cancelled",
    }

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        account_id: str,
        project_id: str,
    ):
        self.api_base = api_base
        self.api_key = api_key
        self.account_id = account_id
        self.project_id = project_id
        self.req_headers = {
            "Authorization": f"Token {self.api_key}",
            "Content-Type": "application/json",
        }
        self.base_url = f"{self.api_base}/api/v2/accounts/{self.account_id}"

    def _get_artifact_url(
        self, *, artifact_name: str, run_id: int, step: str = None
    ) -> str:
        """
        Helper method to get the artifact API url

        :param artifact_name: file name of the artifact
        :return: artifact API url
        """
        return (
            self._get_run_url(run_id=run_id)
            + f"artifacts/{artifact_name}"
            + (f"?step={step}" if step else "")
        )

    def _get_job_url(self, *, job_id: int) -> str:
        """
        Helper method to get the job API url

        :param job_id:
        :return:
        """
        return f"{self.base_url}/jobs/{job_id}/run/"

    def _get_job_artifact_url(self, *, job_id: int, artifact: str) -> str:
        """
        Helper method to get the job API url

        :param job_id:
        :return:
        """
        return f"{self.base_url}/jobs/{job_id}/artifacts/{artifact}"

    def _get_run_url(self, *, run_id: int) -> str:
        """
        Helper method to get the run API url

        :param run_id: dbt cloud run identifier
        :return: run API url
        """
        return f"{self.base_url}/runs/{run_id}/"

    def get_status_link(
        self,
        *,
        run_id: int,
    ) -> str:
        """
        Returns the direct link to the dbt cloud run which can be opened in a browser

        :param run_id: dbt cloud run identifier
        :return: direct link to the dbt cloud run
        """
        return f"{self.api_base}/deploy/{self.account_id}/projects/{self.project_id}/runs/{run_id}/"

    def run_job(
        self,
        *,
        job_id: int,
        job_cause: str,
        git_branch: str = None,
        git_sha: str = None,
        schema_override: str = None,
        steps_override: list[str] = None,
        azure_pull_request_id: int = None,
    ) -> int:
        """
        Runs a dbt cloud job using the dbt cloud API

        :param job_id: dbt cloud job identifier
        :param job_cause: reason for running the job
        :param git_branch: (optional) git branch to run the job from, either git_branch or git_sha can be provided
        :param git_sha: (optional) git sha to run the job from, either git_branch or git_sha can be provided
        :param schema_override: (optional) schema override to run the job with
        :param steps_override: (optional) steps override to run the job with, should be an array of dbt commands
        :param azure_pull_request_id: (optional) identifier of the Azure DevOps pull request
        :return: dbt cloud run identifier
        """
        if git_branch and git_sha:
            raise ValueError("Either git_branch or git_sha can be provided, not both")

        req_job_url = self._get_job_url(job_id=job_id)
        req_payload = {"cause": job_cause}
        if git_branch:
            req_payload["git_branch"] = git_branch.replace("refs/heads/", "")
        if git_sha:
            req_payload["git_sha"] = git_sha
        if schema_override:
            req_payload["schema_override"] = schema_override.replace("-", "_")
        if steps_override:
            req_payload["steps_override"] = steps_override
        if azure_pull_request_id:
            req_payload["azure_pull_request_id"] = azure_pull_request_id

        data = json.dumps(req_payload).encode()
        request = Request(
            method="POST", data=data, headers=self.req_headers, url=req_job_url
        )

        with urlopen(request) as req:
            response = req.read().decode("utf-8")
            run_job_resp = json.loads(response)

        return run_job_resp["data"]["id"]

    def cancel_run(self, run_id: int) -> bool:
        """
        Cancels a dbt cloud job run using the dbt cloud API

        :param run_id: Identifier of the run to cancel
        :return: boolean indicating if the job was successfully cancelled
        """
        req_run_url = f"{self._get_run_url(run_id=run_id)}/cancel/"

        request = Request(method="POST", headers=self.req_headers, url=req_run_url)

        with urlopen(request) as req:
            response = req.read().decode("utf-8")
            cancel_job_resp = json.loads(response)

        return cancel_job_resp["status"]["is_success"]

    def get_run_status(
        self,
        *,
        run_id: int,
    ) -> str:
        """
        Retrieves the status of a specific dbt cloud job run using the dbt cloud API

        :param run_id: dbt cloud  run identifier
        :return: dbt cloud run status for a specific run
        """
        req_run_url = self._get_run_url(run_id=run_id)
        request = Request(headers=self.req_headers, url=req_run_url)

        with urlopen(request) as req:
            response = req.read().decode("utf-8")
            req_status_resp = json.loads(response)

        run_status_code = req_status_resp["data"]["status"]
        return self.run_status_map[run_status_code]

    def get_latest_job_artifact(
        self, *, job_id: int, artifact: str, artifact_path: str
    ) -> None:
        """
        Retrieves the latest artifact for a specific job using the dbt cloud API

        :param job_id: dbt cloud job identifier
        :param artifact: file name of the artifact to retrieve
        :param artifact_path: path of the artifact
        :return: dbt cloud run artifact
        """
        req_job_url = self._get_job_artifact_url(job_id=job_id, artifact=artifact)
        request = Request(headers=self.req_headers, url=req_job_url)

        with urlopen(request) as req:
            artifact_content = req.read().decode("utf-8")

        # Save the artifact content to the specified path
        with open(artifact_path, "w") as artifact_file:
            artifact_file.write(artifact_content)

    def poll_run_status(
        self,
        *,
        run_id: int,
        poll_interval: int = 10,
    ):
        """
        Polls the DBT cloud API on an interval for the status of a specific dbt cloud job run
        Raises an exception if the run fails or is canceled

        :param run_id: dbt cloud run identifier
        :param poll_interval: (optional) interval to poll the dbt cloud API for the run status
        :return: None
        """
        time.sleep(poll_interval)

        while True:
            status = self.get_run_status(run_id=run_id)
            logging.info(f"Run status -> {status}")
            if status in ["Error", "Cancelled"]:
                run_status_link = self.get_status_link(run_id=run_id)
                raise Exception(f"Run failed or canceled. See why at {run_status_link}")
            if status == "Success":
                return
            time.sleep(poll_interval)

    def get_run_artifact(
        self, *, run_id: int, artifact: str, artifact_path: str, step: str
    ):
        """
        Retrieves the contents of a specific artifact from dbt cloud using the dbt cloud API

        :param run_id: dbt cloud run identifier
        :param artifact: file name of the artifact to retrieve
        :param artifact_path: path of the artifact
        :param step: dbt cloud run step of the artifact
        :return: contents of the artifact
        """
        artifact_url = self._get_artifact_url(
            artifact_name=artifact, run_id=run_id, step=step
        )
        request = Request(headers=self.req_headers, url=artifact_url)

        with urlopen(request) as req:
            artifact_content = req.read().decode("utf-8")

        # Save the artifact content to the specified path
        with open(artifact_path, "w") as artifact_file:
            artifact_file.write(artifact_content)
