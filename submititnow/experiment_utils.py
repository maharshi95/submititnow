import argparse
import os
import time
import datetime as dt
from pathlib import Path
from typing import List, Iterable, Optional, Callable, Any, Dict

from rich import print as rich_print
from rich.live import Live
from rich.table import Table
import submitit


__FALLBACK_SUBMITITNOW_DIR = os.path.expanduser("~/.submititnow")


def get_default_submititnow_dir():
    return Path(os.environ.get("SUBMITITNOW_DIR", __FALLBACK_SUBMITITNOW_DIR))


def add_submitit_params(parser: argparse.ArgumentParser):
    slurm_group = parser.add_argument_group("Submitit parameters")
    slurm_group.add_argument("--slurm_account", default=None, help="SLURM account")
    slurm_group.add_argument("--slurm_partition", default=None, help="SLURM partition")
    slurm_group.add_argument("--slurm_qos", default=None, help="SLURM qos")
    slurm_group.add_argument(
        "--slurm_mem", default=None, help="SLURM memory requirement"
    )
    slurm_group.add_argument(
        "--slurm_gres", default=None, help="SLURM GPU Resource requirement"
    )
    slurm_group.add_argument(
        "--slurm_time", default=None, help="SLURM time requirement"
    )
    parser.add_argument("--exp_name", default=None, help="Experiment Name")
    parser.add_argument("--submititnow_dir", default=None, help="base submitit log dir")


def add_umiacs_params(parser: argparse.ArgumentParser):
    # TODO(mgor): Make these generalizable to other clusters by creating registerable profile-handlers.
    umiacs_group = parser.add_argument_group("UMIACS parameters")
    umiacs_group.add_argument(
        "--scavenger",
        action="store_true",
        help="Boolean flag to run experiments in scavenger mode",
    )
    umiacs_group.add_argument(
        "--clip",
        action="store_true",
        help="Boolean flag to run experiments using clip account",
    )


def get_submititnow_log_dir(args: argparse.Namespace):
    return (
        Path(args.submititnow_dir)
        if args.submititnow_dir
        else get_default_submititnow_dir()
    )


def get_slurm_params(args: argparse.Namespace):
    slurm_params = {k: v for k, v in vars(args).items() if k.startswith("slurm_")}

    if args.scavenger:
        slurm_params[f"slurm_account"] = "scavenger"

    if args.clip:
        slurm_params[f"slurm_account"] = "clip"

    return slurm_params


def _job_start_time(job: submitit.Job):
    return dt.datetime.fromtimestamp(job._start_time)


def _display_job_submission_status_on_console(exp: "Experiment", wait_until: str):
    print()
    # fmt: off
    rich_print(f" \t:rocket: [bold]Launched {len(exp.jobs)} job(s)[/bold] :rocket: ")
    print()
    rich_print(f"\t:mag: "
               f"Experiment ID      : [bold bright_cyan]{exp.exp_id}\n")
    rich_print(f"\t:test_tube: "
               f"Experiment name    : [bold bright_cyan]{exp.exp_name}\n")
    rich_print(f"\t:bar_chart: "
               f"Experiment tracker : {exp.tracker_file}\n")
    rich_print(f"\t:ledger: "
               f"Submitit logs      : {exp.logs_dir}\n")

    rich_print(f"[bold yellow]  Execute the following command to monitor the jobs:[/bold yellow]\n")
    rich_print(f"\t[bold bright_white]jt jobs {exp.exp_name} {exp.exp_id}[/bold bright_white]\n")
    # fmt: on
    def generate_console_table():
        table = Table(show_header=True, highlight=True)
        table.add_column("JobID", justify="right", style="bold cyan", no_wrap=True)
        table.add_column("Experiment Configs")
        table.add_column("Job Configs")
        table.add_column("State")
        table.add_column("Nodelist")

        for job_id, job in exp.jobs.items():
            exp_description = exp.job_function_description
            job_params_info = exp.job_descriptions[job_id]
            jobs_info = job.get_info()
            if not jobs_info:
                job_state = "UNKNOWN"
                nodelist = f"[dark_orange]UNKNOWN"
            else:
                print(jobs_info)
                job_state = jobs_info["State"]
                nodelist = jobs_info["NodeList"]

            state_color = {
                "UNKNOWN": "dark_orange",
                "PENDING": "yellow",
                "RUNNING": "bright_green",
                "COMPLETED": "bold green4",
                "FAILED": "bold red",
            }[job_state]

            job_state_decorated = f"[{state_color}]{job_state}"
            # if job_state == 'FAILED':
            #     job_state_decorated = ":skull: " + job_state_decorated
            # if job_state == 'COMPLETED':
            #     job_state_decorated = ":tada: " + job_state_decorated
            row_text = (
                f"{job_id}",
                f"{exp_description}",
                job_params_info,
                job_state_decorated,
                nodelist,
            )
            table.add_row(*row_text)
        return table

    waiting_states = set()
    if wait_until == "submitted":
        waiting_states = {"UNKNOWN"}
    if wait_until == "running":
        waiting_states = {"UNKNOWN", "PENDING"}
    if wait_until == "done":
        waiting_states = {"UNKNOWN", "PENDING", "RUNNING"}

    with Live(generate_console_table(), refresh_per_second=10) as live:
        jobs_in_queue = {*exp.jobs.values()}
        jobs_submitted = set()
        wait_over = False
        while not wait_over:
            wait_over = True
            for job in jobs_in_queue:
                if job.state in waiting_states:
                    wait_over = False
                else:
                    jobs_submitted.add(job)
                live.update(generate_console_table())
            jobs_in_queue = jobs_in_queue - jobs_submitted
            time.sleep(0.1)


class Experiment:
    def __init__(
        self,
        exp_name: str,
        job_func: Callable,
        job_params: Iterable[argparse.Namespace],
        job_desc_function: Optional[Callable] = None,
        submititnow_dir: Optional[str] = None,
    ):

        self.submititnow_dir = (
            Path(submititnow_dir) if submititnow_dir else get_default_submititnow_dir()
        )
        self.exp_name = exp_name
        self.job_func = job_func
        self.job_params = list(job_params)
        self.job_desc_function = job_desc_function or (lambda x: str(x))
        self.jobs = {}
        self.job_descriptions = {}

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
        return self.submititnow_dir / "experiments" / self.exp_name

    @property
    def tracker_file(self):
        return self.exp_dir / "tracker.csv"

    @property
    def logs_dir(self):
        return self.exp_dir / "submitit_logs"

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
        if slurm_params["slurm_account"] == "scavenger":
            slurm_params[f"slurm_partition"] = "scavenger"
            slurm_params[f"slurm_qos"] = "scavenger"

        elif slurm_params["slurm_account"] == "clip":
            slurm_params[f"slurm_partition"] = "clip"

        self.executor = submitit.AutoExecutor(self.logs_dir)
        self.executor.update_parameters(**slurm_params)

        jobs = self.executor.map_array(self.job_func, self.job_params)
        job_descriptions = map(self.job_desc_function, self.job_params)

        self._assign_jobs(jobs, job_descriptions)

        if verbose:
            _display_job_submission_status_on_console(self, wait_until)

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

        with open(self.tracker_file, "a") as fp:
            row_items = [
                job_start_time_str,
                str(job.job_id),
                job_desc,
                self.job_function_description,
            ]
            fp.write("\t".join(row_items))
            fp.write("\n")
