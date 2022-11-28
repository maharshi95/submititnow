# submititnow
A "makeshift" toolkit to create and launch Jobs from existing python scripts on SLURM using [submitit](https://github.com/facebookincubator/submitit), and also interactively monitor the status of jobs running currently or terminated over CLI.

# Installation
```
pip install -U git+https://github.com/maharshi95/submititnow.git
```

# **`slaunch`**: &nbsp; Launching a python script on SLURM using `submititnow`

Let's say you have a python script `examples/annotate_queries.py` that can be run using following command:

```
python examples/annotate_queries.py --model='BERT-LARGE-uncased' \
    --dataset='NaturalQuestions' --fold='dev'
```
You can launch a job that runs this script over a SLURM cluster using following:
```
slaunch examples/annotate_queries.py \
    --slurm_mem="16g" --slurm_gres="gpu:rtxa4000:1" \
    --model='BERT-LARGE-uncased' --dataset='NaturalQuestions' --fold='dev'
```

More so, once can easily sweep over a subset of arguments to launch multiple jobs with little to no effort:
```
slaunch examples/annotate_queries.py \
    --slurm_mem="16g" --slurm_gres="gpu:rtxa4000:1" \
    --sweep fold model \
    --model 'BERT-LARGE-uncased' 'Roberta-uncased' 'T5-cased-small' \
    --dataset='NaturalQuestions' --fold 'dev' 'train'
```
Above command will launch a total of 6 jobs with following configuration:

![Slaunch Terminal Response](docs/imgs/slaunch_annotate_queries.png)

# **`jt`**: &nbsp; Looking up info on previously launched experiments:

As instructed in the screenshot of the Launch response, user can utilize the `jt` (short of `job-tracker`) command to monitor the job progress.

## **`jt jobs EXP_NAME [EXP_ID]`**

Executing `jt jobs examples.annotate_queries 227720` will give following response:

![jt jobs EXP_NAME EXP_ID Terminal Response](docs/imgs/jt_annotate_queries_expid.png)

In fact, user can also lookup all `examples.annotate_queries` jobs simply by removing the `[EXP_ID]` from the previous command:
```
jt jobs examples.annotate_queries
```
![jt jobs EXP_NAME Terminal Response](docs/imgs/jt_annotate_queries.png)

## **`jt {err, out} JOB_ID`**

### Looking up stderr and stdout of a Job

Executing `jt out 227720_2` reveals the `stdout` output of the corresponding Job:

![jt out JOB_ID Terminal Response](docs/imgs/jt_out_job_id.png)
Similar is case for `jt err 227720_2` which reveals `stderr` logs.

## **`jt sh JOB_ID`**
### Looking up SLURM SBATCH shell file of a Job

submitit tool internally create an SBATCH shell script per experiment to launch the jobs on SLURM cluster. This command helps inspect this `submission.sh` file.

Executing `jt sh 227720_2` reveals the following:

![jt out JOB_ID Terminal Response](docs/imgs/jt_sh_job_id.png)

## **`jt ls`**
Finally, user can use `jt ls` to simply list the experiments maintains by the `submititnow` tool.

<img src="docs/imgs/jt_ls.png"  width=30%>

Outputs of this command can be further used to interact using `jt jobs` command.