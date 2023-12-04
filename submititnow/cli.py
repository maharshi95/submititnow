from __future__ import annotations
import io
import time

from rich import print as rich_print
from rich.live import Live
from rich.table import Table

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from submititnow.experiment_lib import Experiment


def show_file_content(filepath: str):
    rich_print(
        "[bold bright_yellow]Reading file:[/bold bright_yellow] [bold cyan]{}[/bold cyan]\n".format(
            filepath
        )
    )
    with open(filepath, "r", newline="") as fp:
        text = fp.read()
        for line in text.split("\n"):
            line_buffer = io.StringIO()
            for chunks in line.split("\r"):
                line_buffer.seek(0)
                line_buffer.write(chunks)
            rich_print(line_buffer.getvalue())


def _generate_console_table(exp: Experiment):
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


def _display_job_submission_status_on_console(exp: Experiment, wait_until: str):
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

    rich_print("[bold yellow]  Execute the following command to monitor the jobs:[/bold yellow]\n")
    rich_print(f"\t[bold bright_white]jt jobs {exp.exp_name} {exp.exp_id}[/bold bright_white]\n")
    # fmt: on

    waiting_states = set()
    if wait_until == "submitted":
        waiting_states = {"UNKNOWN"}
    if wait_until == "running":
        waiting_states = {"UNKNOWN", "PENDING"}
    if wait_until == "done":
        waiting_states = {"UNKNOWN", "PENDING", "RUNNING"}

    with Live(_generate_console_table(exp), refresh_per_second=10) as live:
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
                live.update(_generate_console_table(exp))
            jobs_in_queue = jobs_in_queue - jobs_submitted
            time.sleep(0.1)
