from sfapi_client import AsyncClient
from sfapi_client.compute import Machine
import asyncio
from pathlib import Path
import json
import logging
logging.basicConfig(level=logging.DEBUG)

key = Path().home() / ".superfacility" / "development.pem"

script = """#!/bin/bash
#SBATCH --constraint=haswell
#SBATCH --nodes=1
#SBATCH --time=00:02:00
#SBATCH --job-name=hello_world
#SBATCH --qos=debug

echo "Hello {}"
"""


async def submit_job(semaphore: asyncio.Semaphore, number: int):
    async with semaphore:
        async with AsyncClient(key=key, wait_interval=30) as client:
            perlmutter = await client.compute(Machine.cori)
            job = await perlmutter.submit_job(script.format(number))
            # Wait for tasks to be completed
            await job.complete()
            return job


async def main(number_to_submit: int = 2, batch_size: int = 1):
    # Create the number of semaphores for the number of simultanious jobs to place
    semaphore = asyncio.Semaphore(batch_size)
    # Submit the jobs based on a list of parameters
    tasks = [submit_job(semaphore, number=number)
             for number in range(number_to_submit)]
    # Gather all the outputs
    task_output = await asyncio.gather(*tasks)
    # Print the job outputs
    print(json.dumps([t.dict() for t in task_output]))


asyncio.run(main())

# To run from a terminal: `python multiple_async_jobs.py | jq`


# ```
# [
#   {
#     "account": "ntrain",
#     "tres_per_node": "gres:craynetwork:1",
#     "min_cpus": "1",
#     "min_tmp_disk": "0",
#     "end_time": "N/A",
#     "features": "haswell",
#     "group": "95745",
#     "over_subscribe": "NO",
#     "jobid": "3394948",
#     "name": "hello_world",
#     "comment": "(null)",
#     "time_limit": "2:00",
#     "min_memory": "0",
#     "req_nodes": "",
#     "command": "(null)",
#     "priority": "73380",
#     "qos": "debug_hsw",
#     "reason": "Priority",
#     "field_": null,
#     "st": "PD",
#     "user": "elvis",
#     "reservation": "(null)",
#     "wckey": "(null)",
#     "exc_nodes": "",
#     "nice": "0",
#     "s_c_t": "*:*:*",
#     "exec_host": "n/a",
#     "cpus": "1",
#     "nodes": "1",
#     "dependency": "(null)",
#     "array_job_id": "3394948",
#     "sockets_per_node": "*",
#     "cores_per_socket": "*",
#     "threads_per_core": "*",
#     "array_task_id": "N/A",
#     "time_left": "2:00",
#     "time": "0:00",
#     "nodelist": "",
#     "contiguous": "0",
#     "partition": "debug_hsw",
#     "nodelist_reason_": "(Priority)",
#     "start_time": "N/A",
#     "state": "COMPLETED",
#     "uid": "95745",
#     "submit_time": "2023-05-22T11:26:13",
#     "licenses": "(null)",
#     "core_spec": "N/A",
#     "schednodes": "(null)",
#     "work_dir": "/global/homes/e/elvis"
#   },
#   {
#     "account": "ntrain",
#     "tres_per_node": "gres:craynetwork:1",
#     "min_cpus": "1",
#     "min_tmp_disk": "0",
#     "end_time": "2023-05-22T11:29:25",
#     "features": "haswell",
#     "group": "95745",
#     "over_subscribe": "NO",
#     "jobid": "3395057",
#     "name": "hello_world",
#     "comment": "(null)",
#     "time_limit": "2:00",
#     "min_memory": "0",
#     "req_nodes": "",
#     "command": "(null)",
#     "priority": "73380",
#     "qos": "debug_hsw",
#     "reason": "Prolog",
#     "field_": null,
#     "st": "R",
#     "user": "elvis",
#     "reservation": "(null)",
#     "wckey": "(null)",
#     "exc_nodes": "",
#     "nice": "0",
#     "s_c_t": "*:*:*",
#     "exec_host": "nid00883",
#     "cpus": "64",
#     "nodes": "1",
#     "dependency": "(null)",
#     "array_job_id": "3395057",
#     "sockets_per_node": "*",
#     "cores_per_socket": "*",
#     "threads_per_core": "*",
#     "array_task_id": "N/A",
#     "time_left": "1:59",
#     "time": "0:01",
#     "nodelist": "nid00883",
#     "contiguous": "0",
#     "partition": "debug_hsw",
#     "nodelist_reason_": "nid00883",
#     "start_time": "2023-05-22T11:27:25",
#     "state": "COMPLETED",
#     "uid": "95745",
#     "submit_time": "2023-05-22T11:27:20",
#     "licenses": "(null)",
#     "core_spec": "N/A",
#     "schednodes": "(null)",
#     "work_dir": "/global/homes/e/elvis"
#   }
# ]
# ```
