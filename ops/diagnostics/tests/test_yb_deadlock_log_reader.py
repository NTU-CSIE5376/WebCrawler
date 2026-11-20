import importlib.util
from pathlib import Path
import select
import subprocess
import sys
import time

SCRIPT = Path(__file__).parents[1] / 'yb_deadlock_log_reader.py'
spec = importlib.util.spec_from_file_location('reader', SCRIPT)
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)
A = 'ba99f472-dded-4b39-87d7-dcba23b462bf'
B = '89aebf79-95ac-4621-b805-004cbd452bb4'

def test_filter_only_reconstructs_ids_without_url_or_sql():
 f=reader.Filter()
 assert f.line(b'2026-11-13 05:07:46 UTC [42] ERROR: deadlock detected https://secret.example/private',1).endswith('ERROR: deadlock detected')
 line=f'2026-11-13 05:07:46 UTC [42] DETAIL: {A}->{B}-> https://secret.example/private'.encode()
 out=f.line(line,1)
 assert A in out and B in out and 'https' not in out and 'secret' not in out
 assert f.line(b"2026-11-13 UTC [42] STATEMENT: INSERT INTO test VALUES ('private')",1) is None

def test_detail_requires_same_recent_pid_and_new_error_clears_state():
 f=reader.Filter();f.line(b'2026-11-13 UTC [1] ERROR: deadlock detected',0)
 assert f.line(f'2026-11-13 UTC [2] DETAIL: {A}'.encode(),1) is None
 assert f.line(f'2026-11-13 UTC [1] DETAIL: {A}'.encode(),11) is None
 f.line(b'2026-11-13 UTC [1] ERROR: deadlock detected',20)
 f.line(b'2026-11-13 UTC [1] ERROR: different error',21)
 assert f.line(f'2026-11-13 UTC [1] DETAIL: {A}'.encode(),21) is None

def test_real_reader_stops_on_stdin_eof_and_skips_existing_lines(tmp_path):
 log=tmp_path/'postgresql-2026-11-13_000000.log'
 log.write_text('2026-11-13 UTC [99] ERROR: deadlock detected\n')
 proc=subprocess.Popen([sys.executable,'-u',str(SCRIPT),str(tmp_path),'--duration-seconds','10'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 try:
  time.sleep(.2)
  with log.open('a') as f:
   f.write(f'2026-11-13 UTC [42] ERROR: deadlock detected\n2026-11-13 UTC [42] DETAIL: {A}->{B}-> https://private.example/a\n')
  assert select.select([proc.stdout],[],[],3)[0]
  first=proc.stdout.readline().decode();assert '[42]' in first and '[99]' not in first
  second=proc.stdout.readline().decode();assert A in second and B in second and 'private' not in second
  proc.stdin.close();assert proc.wait(timeout=3)==0
 finally:
  if proc.poll() is None:proc.kill();proc.wait(timeout=3)
