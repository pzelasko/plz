from dask.distributed import Client
from dask_jobqueue import SGECluster


def run(
        fn,
        *inputs,
        memory='2G',
        gpus=0,
        log_dir=None,
        timeout_s=str(3600 * 24 * 7),  # a week
        proc_per_worker=1,
        cores_per_proc=1,
        env_extra=None,
        job_extra=None,
        grid='clsp'

):
    with setup_cluster(
            memory=memory,
            gpus=gpus,
            log_dir=log_dir,
            timeout_s=timeout_s,
            proc_per_worker=proc_per_worker,
            cores_per_proc=cores_per_proc,
            env_extra=env_extra,
            job_extra=job_extra,
            grid=grid
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(1)
            future = client.submit(fn, *inputs)
            return future.result()


def map(
        fn,
        *inputs,
        jobs=1,
        memory='2G',
        gpus=0,
        log_dir=None,
        timeout_s=str(3600 * 24 * 7),  # a week
        proc_per_worker=1,
        cores_per_proc=1,
        env_extra=None,
        job_extra=None,
        grid='clsp'
):
    with setup_cluster(
            memory=memory,
            gpus=gpus,
            log_dir=log_dir,
            timeout_s=timeout_s,
            proc_per_worker=proc_per_worker,
            cores_per_proc=cores_per_proc,
            env_extra=env_extra,
            job_extra=job_extra,
            grid=grid
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(jobs)
            futures = client.map(fn, *inputs)
            results = client.gather(futures)
    return results


def setup_cluster(
        memory='2G',
        gpus=0,
        log_dir=None,
        timeout_s=str(3600 * 24 * 7),  # a week
        proc_per_worker=1,
        cores_per_proc=1,
        env_extra=None,
        job_extra=None,
        grid='clsp',
        *args,
        **kwargs
) -> SGECluster:
    if env_extra is None:
        env_extra = []
    # We're creating the "qsub"-like resource specifiation here
    resource_spec = ''
    queue = 'all.q'

    if grid == 'clsp':
        # Add memory specification (CLSP grid specific)
        qsub_mem_str = f'mem_free={memory},ram_free={memory}'.replace('GB', 'G')
        
        # Handle GPU jobs
        if gpus:
            # Nun GPUs arg + limit hosts to c nodes (with PyTorch compatible GPUs)
            resource_spec += f',gpu={gpus},hostname=c*'
            # Set the queu as needed
            queue = 'g.q'
            # Check which GPU is free to use
            env_extra.append(f'export CUDA_VISIBLE_DEVICES=$(free-gpu -n {gpus})')
    
    elif grid == 'coe':
        # Add memory specification (CLSP grid specific)
        qsub_mem_str = f'mem_free={memory}'.replace('GB', 'G')
        
        # Handle GPU jobs
        if gpus:
            # Nun GPUs arg + limit hosts to c nodes (with PyTorch compatible GPUs)
            resource_spec += f',gpu={gpus}'
            # Set the queu as needed
            queue = 'gpu.q'
    
    resource_spec += qsub_mem_str
    # Create a "mini cluster" that our jobs will get submitted to
    return SGECluster(
        queue=queue,
        walltime=timeout_s,
        processes=proc_per_worker,
        memory=memory,
        cores=cores_per_proc,
        resource_spec=resource_spec,
        log_directory=log_dir if log_dir is not None else 'log',
        job_extra=job_extra,
        env_extra=env_extra,  # e.g. ['export ENV_VARIABLE="SOMETHING"', 'source myscript.sh']
        *args,
        **kwargs,
    )