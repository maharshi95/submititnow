import argparse, itertools

import submititnow

# Import the target module
from examples import demo_script


def create_job_params():
    job_params = []

    for num1, num2 in itertools.product(range(1, 4), range(5, 8)):
        job_params.append(argparse.Namespace(num1=num1, num2=num2))

    for num1, num2 in itertools.product(range(4, 7), range(20, 22)):
        job_params.append(argparse.Namespace(num1=num1, num2=num2))

    for job_param in job_params:
        job_param.output = f"output_{job_param.num1}_{job_param.num2}"

    return job_params


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # Add submititnow arguments [optional]
    # Uncomment below line only if you want to make SLURM parameters configurable from the command line.
    # submititnow.add_slurm_arguments(parser)

    # Add slurm arguments [optional]
    # Uncomment below line only if you want to make 'exp_name' and 'submititnow_dir' configurable from the command line.
    # submititnow.add_submititnow_arguments(parser)

    args = parser.parse_args()

    # Create a job description function [optional]
    # This function is used to create a job description for each job.
    # It consumes the job parameters and returns a string.

    # job_description_function = lambda x: f"{vars(x)}"

    experiment = submititnow.Experiment(
        name="demo_script_custom",
        job_func=demo_script.main,
        job_params=create_job_params(),
        # job_desc_function=job_description_function, # Uncomment if you want to use a custom job description function.
    )

    slurm_params = {}

    # uncomment if you want to make SLURM parameters configurable from the command line
    # slurm_params = submititnow.get_slurm_params(args)

    slurm_params["slurm_mem"] = "1G"
    slurm_params["slurm_time"] = "00:05:00"

    experiment.launch(slurm_params)
