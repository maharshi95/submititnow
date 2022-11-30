import argparse
from typing import Dict, Any


def add_slurm_arguments(parser: argparse.ArgumentParser):
    slurm_group = parser.add_argument_group("SLURM parameters")
    slurm_group.add_argument(
        "--slurm_profile", default=None, help="SubmititNow profile for SLURM."
    )
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
    return parser


def add_submititnow_arguments(parser: argparse.ArgumentParser):
    submititnow_group = parser.add_argument_group("SubmititNow parameters")
    submititnow_group.add_argument("--exp_name", default=None, help="Experiment Name.")
    submititnow_group.add_argument(
        "--submititnow_dir", default=None, help="Root directory for submititnow."
    )
    return parser


def get_slurm_params(args: argparse.Namespace) -> Dict[str, Any]:
    return {k: v for k, v in vars(args).items() if k.startswith("slurm_")}
