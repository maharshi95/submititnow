# :rocket: Submit it Now! :rocket:

&nbsp;[![License](https://img.shields.io/github/license/maharshi95/submititnow)](/LICENSE)
&nbsp;[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
&nbsp;[![Supported Python Versions](https://img.shields.io/badge/python-3.8+-blue)](https://pypi.org/project/rich/)
&nbsp;[![Twitter Follow](https://img.shields.io/twitter/follow/maharshigor.svg?style=social)](https://twitter.com/maharshigor)

A _makeshift_ toolkit, built on top of [submitit](https://github.com/facebookincubator/submitit), to launch SLURM jobs over a range of hyperparameters from the command line. It is designed to be used with existing Python scripts and interactively monitor their status.

__`submititnow` provides two command-line tools:__

* `slaunch` to launch a Python script as SLURM job(s).
* `jt` (job-tracker) to interactively monitor the jobs.

__It also provides an abstracted [`experiment_lib.Experiment`](submititnow/experiment_lib.py#L16) API to create, launch and monitor an experiment, or a group of job(s), from Python scripts with customized parameter-sweeping configurations, while being able to track them with `jt`.__

## `slaunch` : Launching a python script over SLURM

Let's say you have a Python script [`examples/annotate_queries.py`](examples/annotate_queries.py) which can be run using the following command:

```bash
python examples/annotate_queries.py --model='BERT-LARGE-uncased' \
    --dataset='NaturalQuestions' --fold='dev'
```

You can launch a job that runs this script over a SLURM cluster using the following:

```bash
slaunch examples/annotate_queries.py \
    --mem="16g" --gres="gpu:rtxa4000:1" \
    --model='BERT-LARGE-uncased' --dataset='NaturalQuestions' --fold='dev'
```

You can put all the slurm params in a config file and pass it to `slaunch` using `--slurm_config` flag. For example, the above command can be written as:

```bash
slaunch examples/annotate_queries.py \
    --config="examples/configs/gpu.json" \
    --model='BERT-LARGE-uncased' --dataset='NaturalQuestions' --fold='dev'
```

### __Launching multiple jobs with parameter-sweep__

```bash
slaunch examples/annotate_queries.py \
    --config="examples/configs/gpu.json" \
    --sweep fold model \
    --model 'BERT-LARGE-uncased' 'Roberta-uncased' 'T5-cased-small' \
    --dataset='NaturalQuestions' --fold 'dev' 'train'
```

This will launch a total of 6 jobs with the following configuration:

![Slaunch Terminal Response](docs/imgs/slaunch_annotate_queries.png)

### __Any constraints on the target Python script that we launch?__

The target Python script must have the following format:

```python
import argparse

# User defined functions and classes

def main(args: argparse.Namespace):
    # Code goes here
    pass


def add_arguments(parser = None) -> argparse.ArgumentParser:
    parser = parser or argparse.ArgumentParser()
    # Return the parser after populating it with arguments.
    return parser


if __name__ == '__main__':
    parser = add_arguments()
    main(parser.parse_args())

```

## __`jt`__ : &nbsp; Looking up info on previously launched experiments:

As instructed in the above screenshot of the Launch response, user can utilize the `jt` (short for `job-tracker`) command to monitor the job progress.

### __`jt jobs EXP_NAME [EXP_ID]`__

Executing `jt jobs examples.annotate_queries 227720` will give the following response:

![jt jobs EXP_NAME EXP_ID Terminal Response](docs/imgs/jt_annotate_queries_expid.png)

In fact, user can also lookup all `examples.annotate_queries` jobs simply by removing `[EXP_ID]` from the previous command:

```bash
jt jobs examples.annotate_queries
```

![jt jobs EXP_NAME Terminal Response](docs/imgs/jt_annotate_queries.png)

### __`jt {err, out} JOB_ID`__

__Looking up stderr and stdout of a Job__

Executing `jt out 227720_2` reveals the `stdout` output of the corresponding Job:

![jt out JOB_ID Terminal Response](docs/imgs/jt_out_job_id.png)
Similarly, `jt err 227720_2` reveals the `stderr` logs.

### __`jt sh JOB_ID`__

__Looking up SBATCH script for a Job__

The submitit tool internally creates an SBATCH shell script per experiment to launch the jobs on a SLURM cluster. This command outputs this `submission.sh` file for inspection.

Executing `jt sh 227720_2` reveals the following:

![jt out JOB_ID Terminal Response](docs/imgs/jt_sh_job_id.png)

### **`jt ls`**

Finally, user can use `jt ls` to simply list the experiments maintained by the `submititnow` tool.

![jt_ls](docs/imgs/jt_ls.png)

The experiment names output by this command can then be passed into the `jt jobs` command.

## __Installing__

Python 3.8+ is required.

```bash
pip install -U git+https://github.com/maharshi95/submititnow.git
```

## **Experiment API**

Sometimes the `slaunch` command-line tool is not enough. For example, one may want to launch a job with customized parameter-sweep configurations, or vary a certain parameter (e.g. `output_filepath`) for each job in the launch. In such cases, one can use the Experiment API provided by `submititnow` to launch jobs from Python scripts and also get the benefits of being able to track them with `jt`.

[examples/launch_demo_script.py](examples/launch_demo_script.py) provides a demo of how to use the `Experiment` API to launch a job with customized parameter-sweep configurations.

```bash
python examples/launch_demo_script.py
```
