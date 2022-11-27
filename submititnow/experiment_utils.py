import argparse
import os
import os.path as osp
import datetime as dt
import functools

import submitit
from typing import List, Iterable, Optional, Callable

DEFAULT_SUBMITIT_LOG_DIR = os.path.expanduser('~/.submitit/logs')


def get_datetime_str():
    return str(dt.datetime.now()).split('.')[0]


def get_default_submitit_log_dir():
    return os.environ.get('SUBMITIT_LOG_DIR', DEFAULT_SUBMITIT_LOG_DIR)


def add_submitit_params(parser: argparse.ArgumentParser):
    slurm_group = parser.add_argument_group('Submitit parameters')
    slurm_group.add_argument('--slurm_account', default=None, help='SLURM account')
    slurm_group.add_argument('--slurm_partition', default=None, help='SLURM partition')
    slurm_group.add_argument('--slurm_qos', default=None, help='SLURM qos')
    slurm_group.add_argument('--slurm_mem', default=None, help='SLURM memory requirement')
    slurm_group.add_argument('--slurm_gres', default=None, help='SLURM GPU Resource requirement')
    slurm_group.add_argument('--slurm_time', default=None, help='SLURM time requirement')
    parser.add_argument('--exp_name', default=None, help='Experiment Name')
    parser.add_argument('--submitit_log_dir', default=None, help='base submitit log dir')


def add_umiacs_params(parser: argparse.ArgumentParser):
    umiacs_group = parser.add_argument_group('UMIACS parameters')
    umiacs_group.add_argument('--scavenger', action=argparse.BooleanOptionalAction, help='Boolean flag to run experiments in scavenger mode')
    umiacs_group.add_argument('--clip', action=argparse.BooleanOptionalAction, help='Boolean flag to run experiments using clip account')


def get_submitit_log_dir(args: argparse.Namespace):
    return args.submitit_log_dir if args.submitit_log_dir else get_default_submitit_log_dir()


def get_slurm_params(args: argparse.Namespace):
    slurm_params = {
        k: v for k, v in vars(args).items() if k.startswith('slurm_')
    }
    
    if args.scavenger:
        slurm_params[f'slurm_account'] = 'scavenger'
    
    if args.clip:
        slurm_params[f'slurm_account'] = 'clip'
    
    return slurm_params


def job_start_time(job):
    return dt.datetime.fromtimestamp(job._start_time)


class Experiment:

    def __init__(
            self,
            exp_name: str,
            job_func: Callable,
            job_params: Iterable[argparse.Namespace],
            job_desc_function: Optional[Callable] = None,
            submitit_log_dir: Optional[str] = None):

        self.submitit_log_dir = submitit_log_dir or get_default_submitit_log_dir()
        self.exp_name = exp_name
        self.job_func = job_func
        self.job_params = job_params
        self.job_desc_function = job_desc_function or (lambda x: str(x))
        self.jobs = {}
        self.descriptions = {}

    @property
    def exp_dir(self):
        return osp.join(self.submitit_log_dir, self.exp_name)

    @property
    def exp_tracker_file(self):
        return self.exp_dir + '.csv'

    def launch(self, **slurm_params):
        
        if slurm_params['slurm_account'] == 'scavenger':
            slurm_params[f'slurm_partition'] = 'scavenger'
            slurm_params[f'slurm_qos'] = 'scavenger'
        
        elif slurm_params['slurm_account'] == 'clip':
            slurm_params[f'slurm_partition'] = 'clip'
        
        self.executor = submitit.AutoExecutor(self.exp_dir)
        self.executor.update_parameters(**slurm_params)

        tasks = [
            functools.partial(self.job_func, param) for param in self.job_params
        ]

        jobs = self.executor.submit_array(tasks)
        job_descriptions = map(self.job_desc_function, self.job_params)

        self.assign_jobs(jobs, job_descriptions)
        print(f'Launched {len(jobs)} jobs.')
        for job in jobs:
            print(job)
        return jobs

    def assign_jobs(self, jobs: List[submitit.Job], job_descriptions: Iterable[str]):
        for job, description in zip(jobs, job_descriptions):
            self.jobs[job.job_id] = job
            self.descriptions[job.job_id] = description

        for job in jobs:
            self.__update_tracker(job, self.descriptions[job.job_id])

    def assign_job(self, job: submitit.Job, description: str):
        self.assign_jobs([job], [description])

    def __update_tracker(self, job, job_desc):
        job_start_time_str = str(job_start_time(job)).split('.')[0]
        with open(self.exp_tracker_file, 'a') as fp:
            fp.write(f"{job_start_time_str}\t{job.job_id}\t{job_desc}\n")
