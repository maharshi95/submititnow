from typing import Dict, Any, Callable


def clip_profile_handler(slurm_params: Dict[str, Any]):
    return {
        **slurm_params,
        "slurm_account": "clip",
        "slurm_partition": "clip",
    }


def scavenger_profile_handler(slurm_params: Dict[str, Any]):
    return {
        **slurm_params,
        "slurm_account": "scavenger",
        "slurm_partition": "scavenger",
        "slurm_qos": "scavenger",
    }
