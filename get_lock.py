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
def file_lock_impl(file: str, stdin : IO[bytes], stdout : Union[IO[bytes],IO[str]], is_shared):
    def do_lock(is_shared):
        opt = "-s" if is_shared else "-x"
        stdin.write(f"exec 9> '{file}'; flock {opt} 9; echo locked;\n".encode()) 
        stdin.flush() # type: ignore
        line = stdout.readline() # type: ignore
        if isinstance(line, bytes):
            line = line.decode()
        if line.strip() != "locked":
            raise Exception(f"Failed to lock {line}")
    do_lock(is_shared)
    yield do_lock

@contextlib.contextmanager
def file_lock(file: str, is_shared = False):
    p = subprocess.Popen(["/bin/bash"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True)
    stdin : IO[bytes] = p.stdin # type: ignore
    stdout : IO[bytes] = p.stdout # type: ignore
    try:
        with file_lock_impl(file, stdin, stdout, is_shared) as do_lock:
            yield do_lock
    finally:
        p.kill()        

@contextlib.contextmanager
def remote_file_lock(file: str, client: Optional[paramiko.SSHClient] = None, remote: Optional[str] = None, is_shared = False):
    if client is None:
        assert(remote is not None)
        client = paramiko.SSHClient()
        client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        client.connect(remote)
    stdin, stdout, _ = client.exec_command("/bin/bash")
    with file_lock_impl(file, stdin, stdout, is_shared) as do_lock:  # type: ignore
        yield do_lock

def get_lock(file: str, is_shared: bool):
    with file_lock(file, is_shared):
        print(f"[get_lock] lock start {file}")
        time.sleep(2)
        print("[get_lock] lock end")

def get_lock2(file: str, remote: str):
    with remote_file_lock(file, remote=remote):
        print(f"[get_lock2] lock start {file=}")
        time.sleep(2)
        print("[get_lock2] lock end")

def change_lock(file: str):
    with file_lock(file, False) as do_lock:
        do_lock(True)
        print(f"[change_lock] lock start {file}")
        time.sleep(2)
        print("[change_lock] lock end")

def change_lock2(file: str):
    with file_lock(file, True) as do_lock:
        do_lock(False)
        print(f"[change_lock2] lock start {file}")
        time.sleep(2)
        print("[change_lock2] lock end")

for i in range(3):
    p1=multiprocessing.Process(target=get_lock, args=("tmp", True))
    p1.start()


for i in range(3):
    time.sleep(1)
    p1=multiprocessing.Process(target=get_lock, args=("tmp", False))
    p2=multiprocessing.Process(target=get_lock2, args=(Path("tmp").absolute(), "localhost"))
    p1.start()
    p2.start()

for i in range(3):
    p1=multiprocessing.Process(target=change_lock, args=("tmp",))
    p1.start()

for i in range(3):
    p1=multiprocessing.Process(target=change_lock2, args=("tmp",))
    p1.start()

