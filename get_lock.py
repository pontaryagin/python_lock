import subprocess
from typing import *
from typing import IO
import fasteners
import multiprocessing
import time
import paramiko
import contextlib
import paramiko
import os
from pathlib import Path

@contextlib.contextmanager
def file_lock_impl(file: str, stdin : IO[bytes], stdout : Union[IO[bytes],IO[str]]):
    stdin.write(f"exec 9> '{file}'; flock -x 9; echo locked;\n".encode()) 
    stdin.flush() # type: ignore
    line = stdout.readline() # type: ignore
    if isinstance(line, bytes):
        line = line.decode()
    if line.strip() != "locked":
        raise Exception(f"Failed to lock {line}")
    yield

@contextlib.contextmanager
def file_lock(file: str):
    p = subprocess.Popen(["/bin/bash"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True,)
    stdin : IO[bytes] = p.stdin # type: ignore
    stdout : IO[bytes] = p.stdout # type: ignore
    try:
        with file_lock_impl(file, stdin, stdout):
            yield
    finally:
        p.kill()        

@contextlib.contextmanager
def remote_file_lock(file: str, client: Optional[paramiko.SSHClient] = None, remote: Optional[str] = None):
    if client is None:
        assert(remote is not None)
        client = paramiko.SSHClient()
        client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        client.connect(remote)
    stdin, stdout, _ = client.exec_command("/bin/bash")
    with file_lock_impl(file, stdin, stdout):  # type: ignore
        yield

def get_lock(file: str, is_shared: bool=False):
    with file_lock(file):
        print(f"[get_lock] lock start {file}")
        time.sleep(2)
        print("[get_lock] lock end")

def get_lock2(file: str, remote: str):
    with remote_file_lock(file, remote="localhost"):
        print(f"[get_lock2] lock start {file=}")
        time.sleep(2)
        print("[get_lock2] lock end")

for i in range(3):
    time.sleep(1)
    p1=multiprocessing.Process(target=get_lock, args=("tmp", False))
    p2=multiprocessing.Process(target=get_lock2, args=(Path("tmp").absolute(), "localhost"))
    p1.start()
    p2.start()


