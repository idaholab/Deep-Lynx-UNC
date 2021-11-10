import os
import subprocess
import requests
from deep_lynx.deep_lynx_service import DeepLynxService


def get_job():
    # call deep lynx
    dlService = DeepLynxService(os.getenv('DEEP_LYNX_URL'), os.getenv('CONTAINER_NAME'),
                                    os.getenv('DATA_SOURCE_NAME'))
    health = dlService.health()
    print(health)

# query deep lynx using provided query

# write file to WRITE_DIR

# set timer for repeat

# FUTURE: run job and return output directly
# # load modules
# modules = ['use.exp_ctl', 'serpent/2.1.31-intel-19.0']

# for module in modules:
#   os.system(f'module load {module}')

# # run serpent
# output = subprocess.Popen(["mpiexec sss2", "-omp", "40", "-nofatal"], stdin="input_file_name.inp", stdout=subprocess.PIPE).communicate()[0]
# print(output)


# set timer for reading from READ_DIR

# if file found, send to deep lynx

# delete file after successful send

# mark job as complete

if __name__ == '__main__':
    get_job()
