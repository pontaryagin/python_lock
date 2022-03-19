import subprocess
import fasteners
import multiprocessing
import time
import paramiko
import contextlib

@contextlib.contextmanager
def lock_file(file: str):
    p = subprocess.Popen(["/bin/bash"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True,)
    try:
        p.stdin.write(f"exec 9> '{file}'; flock -x 9; echo locked;\n ".encode()) # type: ignore
        p.stdin.flush() # type: ignore
        p.stdout.readline() # type: ignore
        yield
    finally:
        p.kill()        

def get_lock(file: str, is_shared: bool=False):
    with lock_file("tmp"):
        print("lock start")
        time.sleep(2)
        print("lock end")

for i in range(10):
    time.sleep(1)
    p1=multiprocessing.Process(target=get_lock, args=("tmp", False))
    p1.start()

