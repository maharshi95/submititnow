import argparse
import json
from typing import Dict, Any


class SlurmAdditionalArgAction(argparse.Action):
    def __init__(self, check_func, *args, **kwargs):
        """
        argparse custom action.
        :param check_func: callable to do the real check.
        """
        self._check_func = check_func
        super(SlurmAdditionalArgAction, self).__init__(*args, **kwargs)

    def __call__(self, parser, namespace, values, option_string):
        if isinstance(values, list):
            values = [self._check_func(parser, v) for v in values]
        else:
            values = self._check_func(parser, values)
        if option_string.startswith("--"):
            option_string = option_string[2:]
        setattr(namespace, self.dest, {option_string: values})


def add_slurm_arguments(parser: argparse.ArgumentParser):
    slurm_group = parser.add_argument_group("SLURM parameters")
    slurm_group.add_argument(
        "--config",
        default=None,
        help="SubmititNow config file for SLURM.",
        dest="slurm_config",
    )
    slurm_group.add_argument(
        "--profile",
        default=None,
        help="SubmititNow profile for SLURM.",
        dest="slurm_profile",
    )
    slurm_group.add_argument(
        "--account", default=None, help="SLURM account", dest="slurm_account"
    )
    slurm_group.add_argument(
        "--partition", default=None, help="SLURM partition", dest="slurm_partition"
    )
    slurm_group.add_argument("--qos", default=None, help="SLURM qos", dest="slurm_qos")
    slurm_group.add_argument(
        "--mem", default=None, help="SLURM memory requirement", dest="slurm_mem"
    )
    slurm_group.add_argument(
        "--gres", default=None, help="SLURM GPU Resource requirement", dest="slurm_gres"
    )
    slurm_group.add_argument(
        "--time", default=None, help="SLURM time requirement", dest="slurm_time"
    )
    slurm_group.add_argument(
        "--nodes",
        default=1,
        help="SLURM nodes requirement",
        dest="slurm_nodes",
        type=int,
    )
    slurm_group.add_argument(
        "--ntasks-per-node",
        default=None,
        help="SLURM ntasks per node",
        dest="slurm_ntasks_per_node",
        type=int,
    )
    slurm_group.add_argument(
        "--cpus-per-task",
        default=None,
        help="SLURM cpus per task",
        dest="slurm_cpus_per_task",
        type=int,
    )
    slurm_group.add_argument(
        "--cpus-per-gpu",
        default=None,
        help="SLURM cpus per gpu",
        dest="slurm_cpus_per_gpu",
        type=int,
    )
    slurm_group.add_argument(
        "--gpus-per-node",
        default=None,
        help="SLURM gpus per node",
        dest="slurm_gpus_per_node",
        type=int,
    )
    slurm_group.add_argument(
        "--gpus-per-task",
        default=None,
        help="SLURM gpus per task",
        dest="slurm_gpus_per_task",
        type=int,
    )

    # Additional arguments
    slurm_group.add_argument(
        "--nodelist",
        default=None,
        help="SLURM nodelist",
        action=SlurmAdditionalArgAction,
        check_func=lambda parser, value: value,
        dest="slurm_additional_parameters",
    )
    return parser


def add_submititnow_arguments(parser: argparse.ArgumentParser):
    submititnow_group = parser.add_argument_group("SubmititNow parameters")
    submititnow_group.add_argument("--exp-name", default=None, help="Experiment Name.")
    submititnow_group.add_argument(
        "--submititnow-dir", default=None, help="Root directory for submititnow."
    )
    return parser


def get_slurm_params(args: argparse.Namespace) -> Dict[str, Any]:
    slurm_args = {
        k: v for k, v in vars(args).items() if k.startswith("slurm_") and v is not None
    }
    if slurm_args.get("slurm_config") is not None:
        config_filename = slurm_args.pop("slurm_config")
        with open(config_filename, "r") as f:
            default_args = json.load(f)
        slurm_args = {**default_args, **slurm_args}
    return slurm_args
