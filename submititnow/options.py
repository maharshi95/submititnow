import argparse
from typing import Dict, Any

all_slurm_arguments = {
    "profile",
    "account",
    "partition",
    "qos",
    "job_name",
    "gres",
    "mem",
    "mem_per_gpu",
    "mem_per_cpu" "gres",
    "time",
    "nodelist",
    "exclude",
}

slurm_args_unsupported_by_submitit = {
    "slurm_nodelist": "nodelist",
}


all_submitit_slurm_arguments = {arg: (f"slurm_" + arg) for arg in all_slurm_arguments}

valid_submitit_slurm_arguments = {
    arg
    for arg in all_slurm_arguments
    if arg not in slurm_args_unsupported_by_submitit.values()
}


def add_slurm_arguments(parser: argparse.ArgumentParser):
    slurm_group = parser.add_argument_group("SLURM parameters")

    for arg_name, slurm_arg_name in all_submitit_slurm_arguments.items():

        arg_group = slurm_group.add_mutually_exclusive_group()
        arg_group.add_argument(
            f"--{arg_name}",
            default=None,
            dest=slurm_arg_name,
            help=f"SLURM {arg_name}",
        )

        arg_group.add_argument(
            f"--slurm_{arg_name}",
            default=None,
            dest=slurm_arg_name,
            help=f"SLURM {arg_name}",
        )

    return parser


def add_submititnow_arguments(parser: argparse.ArgumentParser):
    submititnow_group = parser.add_argument_group("SubmititNow parameters")
    submititnow_group.add_argument("--exp_name", default=None, help="Experiment Name.")
    submititnow_group.add_argument(
        "--submititnow_dir", default=None, help="Root directory for submititnow."
    )
    return parser


def get_slurm_params(args: argparse.Namespace) -> Dict[str, Any]:
    slurm_param_names = set(all_submitit_slurm_arguments.values())
    return {k: v for k, v in vars(args).items() if k in slurm_param_names}
