from typing import Dict, Any, Callable


def clip_profile_handler(slurm_params: Dict[str, Any]):
    return {
        "slurm_account": "clip",
        "slurm_partition": "clip",
        **slurm_params,
    }


def scavenger_profile_handler(slurm_params: Dict[str, Any]):
    return {
        "slurm_account": "scavenger",
        "slurm_partition": "scavenger",
        "slurm_qos": "scavenger",
        **slurm_params,
    }


def cml_zhou_profile_handler(slurm_params: Dict[str, Any]):
    return {
        "slurm_account": "cml-zhou",
        "slurm_partition": "cml-dpart",
        **slurm_params,
    }


def cml_profile_handler(slurm_params: Dict[str, Any]):
    return {
        "slurm_account": "cml",
        "slurm_partition": "cml-dpart",
        **slurm_params,
    }


def cml_scavenger_profile_handler(slurm_params: Dict[str, Any]):
    return {
        "slurm_account": "cml-scavenger",
        "slurm_partition": "cml-scavenger",
        "slurm_qos": "cml-scavenger",
        **slurm_params,
    }


profile_handlers = {
    "clip": clip_profile_handler,
    "scavenger": scavenger_profile_handler,
    "cml": cml_profile_handler,
    "cml-zhou": cml_zhou_profile_handler,
    "cml-scavenger": cml_scavenger_profile_handler,
}
