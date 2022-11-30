import os
import glob
from dataclasses import dataclass
from typing import Optional, Dict
import pandas as pd

from submititnow.experiment_utils import get_default_submititnow_dir

submititnow_root = get_default_submititnow_dir()

experiments_root = submititnow_root / 'experiments'


def get_running_job_ids():
    username = os.environ['USER']
    squeue_rows = os.popen(f'squeue -u {username}').read().splitlines()[1:]
    return list(map(lambda x: x.strip().split()[0].split('_')[0], squeue_rows))


def list_files(path):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            yield os.path.join(r, file)


def find_job_files(job_id, task_id):
    files = {}
    job_id_tag = f'{job_id}_{task_id}' if task_id is not None else str(job_id)
    for path in list_files(experiments_root):
        if path.endswith('.sh') and str(job_id) in path:
            files['sh'] = path
        elif job_id_tag in path:
            name = ext = path.rsplit('.')[-1]
            if ext == 'pkl':
                tag = name.rsplit('_')[-1]
                files[tag] = path
            else:
                files[ext] = path
    return files


def get_job_filepaths(job_task: str) -> Dict[str, str]:
    if '_' in job_task:
        job_id, task_id = job_task.split('_')
    else:
        job_id, task_id = job_task, 0
    return find_job_files(job_id, task_id)


def get_job_filepath(job_task: str, file_type: str) -> str:
    return get_job_filepaths(job_task)[file_type]


def load_job_states(job_id):
    job_id = str(job_id)
    filepaths = get_job_filepaths(job_id)
    running_job_ids = get_running_job_ids()
    
    if 'sh' not in filepaths:
        return 'UNSUBMITTED'

    if 'out' not in filepaths and 'sh' in filepaths:
        if job_id.split('_')[0] in running_job_ids:
            return 'PENDING'
        else:
            return 'CANCELLED (before starting execution)'

    out_filepath = filepaths['out']
    err_filepath = filepaths['err']

    with open(out_filepath) as fp:
        out_lines = list(filter(lambda l: l.startswith('submitit '), fp.readlines()))

    with open(err_filepath) as fp:
        err_lines = list(filter(lambda l: l.startswith('srun: ') or l.startswith('slurmstepd: '), fp.readlines()))

    if not out_lines:
        return 'PENDING'
    
    prefix, msg = out_lines[-1].split(' - ')

    def get_error_msg():
        return err_lines[-1].split(':', 4)[-1].strip()

    if 'completed successfully' in msg:
        return 'completed'.upper()

    elif 'triggered an exception' in msg:
        return 'FAILED: Triggered an Exception'

    if err_lines and 'error' in err_lines[-1]:
        if 'CANCELLED' in err_lines[-1]:
            return 'CANCELLED (terminated by user)'
        else:
            return 'FAILED: ' + get_error_msg()

    if 'Loading' in msg or 'Starting' in msg:
        return 'RUNNING'

    return msg


@dataclass
class JTExp:
    exp_name: str

    @property
    def exp_dir(self):
        return experiments_root / self.exp_name

    @property
    def tracker_file(self):
        return self.exp_dir / 'tracker.csv'

    @property
    def logs_dir(self):
        return self.exp_dir / 'submitit_logs'

    def exists(self):
        return self.exp_dir.exists() and self.tracker_file.exists() and self.logs_dir.exists()

    def get_job_ids(self):
        return list(map(lambda x: x.strip().split()[0].split('_')[0], os.popen(f'squeue -u mgor | grep {self.exp_name}').read().splitlines()[1:]))

    def get_job_states(self):
        job_ids = self.get_job_ids()
        return list(map(load_job_states, job_ids))

    def get_job_states_df(self):
        job_ids = self.get_job_ids()
        job_states = self.get_job_states()
        return pd.DataFrame({'job_id': job_ids, 'state': job_states})

    def show_job_states(self):
        df = self.get_job_states_df()
        print(df)

    def load_csv(self):
        col_names = ['Date & Time', 'Job ID', 'Job Description', 'Exp Info']
        df = pd.read_csv(self.tracker_file, delimiter='\t', names=col_names, header=None)
        df['Exp Info'] = df['Exp Info'].fillna('Not Found in tracker')
        job_series = df['Job ID'].map(lambda x: int(str(x).split('_')[0]))
        df.insert(0, 'Exp ID', job_series)
        # df = df.set_index('job', append=True).swaplevel(0,1)
        return df


def load_job_trackers(exp_name: Optional[str] = None):
   # TODO(mgor): implement loading all trackers if exp_name is None
   pass
