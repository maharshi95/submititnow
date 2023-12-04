import argparse
import datetime as dt
from pathlib import Path
from typing import List, Iterable, Optional, Callable, Any, Dict

import submitit

from submititnow import cli
from submititnow.jt import utils


def _job_start_time(job: submitit.Job):
    return dt.datetime.fromtimestamp(job._start_time)


class Experiment:
    def __init__(
        self,
        name: str,
        job_func: Callable,
        job_params: Iterable[argparse.Namespace],
        job_desc_function: Optional[Callable] = None,
        submititnow_dir: Optional[str] = None,
    ):
        self.submititnow_dir = (
            Path(submititnow_dir) if submititnow_dir else utils.SUBMITITNOW_ROOT_DIR
        )
        self.exp_name = name
        self.job_func = job_func
        self.job_params = list(job_params)
        self.job_desc_function = job_desc_function or (lambda x: str(x))
        self.jobs = {}
        self.job_descriptions = {}
        self.profile_handlers = {}

    @property
    def job_function_description(self):
        func_name = self.job_func.__module__ + "." + self.job_func.__qualname__

        common_params = vars(self.job_params[0])
        for param in self.job_params[1:]:
            for k, v in vars(param).items():
                if k in common_params and v != common_params[k]:
                    del common_params[k]

        tokens = []
        for k, v in common_params.items():
            token = f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}"
            tokens.append(token)
        return f"{func_name}( {', '.join(tokens)} )"

    @property
    def exp_dir(self):
        return utils.EXPERIMENTS_ROOT_DIR / self.exp_name

    @property
    def tracker_file(self):
        return self.exp_dir / "tracker.csv"

    @property
    def logs_dir(self):
        return self.exp_dir / "submitit_logs"

    def register_profile_handler(
        self, profile: str, handler: Callable[[Dict[str, Any]], Dict[str, Any]]
    ):
        self.profile_handlers[profile] = handler

    def launch(
        self,
        slurm_params: Dict[str, Any],
        *,
        verbose: bool = True,
        wait_until: str = "submitted",
    ):
        """Launches the experiment on the cluster. If `wait_until` is None, the function returns immediately.

        Args:
            slurm_params (dict): Dictionary of slurm parameters.
            verbose: Boolean flag to print job status. Optional, defaults to True
            wait_until:. Defaults to 'submitted'. Options are 'none', 'submitted', 'running', 'done'

        Returns:
            list: List of SLURMJob objects
        """
        if wait_until not in {"none", "submitted", "running", "done"}:
            raise ValueError(
                f"wait_until must be one of 'none', 'submitted', 'running', 'done', got {wait_until}"
            )

        if slurm_profile := slurm_params.get("slurm_profile"):
            del slurm_params["slurm_profile"]

            if slurm_profile in self.profile_handlers:
                slurm_params = self.profile_handlers[slurm_profile](slurm_params)
            else:
                raise ValueError(
                    f"Profile {slurm_profile} is not registered. "
                    f"Please register it using `experiment.register_profile_handler`, or use a valid profile. [Valid profiles: {list(self.profile_handlers.keys())}]"
                )

        self.executor = submitit.AutoExecutor(self.logs_dir)
        self.executor.update_parameters(**slurm_params)

        jobs = self.executor.map_array(self.job_func, self.job_params)
        job_descriptions = map(self.job_desc_function, self.job_params)

        self._assign_jobs(jobs, job_descriptions)

        if verbose:
            cli._display_job_submission_status_on_console(self, wait_until)

        return jobs

    def _assign_jobs(self, jobs: List[submitit.Job], job_descriptions: Iterable[str]):
        self.exp_id = jobs[0].job_id.split("_")[0]
        for job, description in zip(jobs, job_descriptions):
            self.jobs[job.job_id] = job
            self.job_descriptions[job.job_id] = description

        for job in jobs:
            self._update_tracker(job, self.job_descriptions[job.job_id])

    def _assign_job(self, job: submitit.Job, description: str):
        self._assign_jobs([job], [description])

    def _update_tracker(self, job, job_desc):
        job_start_time_str = str(_job_start_time(job)).split(".")[0]
        self.tracker_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.tracker_file, "a") as fp:
            row_items = [
                job_start_time_str,
                str(job.job_id),
                job_desc,
                self.job_function_description,
            ]
            fp.write("\t".join(row_items))
            fp.write("\n")
