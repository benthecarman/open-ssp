#!/usr/bin/env python3
"""Validate an extracted bundle read-only, offline, outside the checkout via bubblewrap.

All host writes stay in --work-dir. Only that directory is mounted writable.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request


def inner():
    runner = '/opt/fixture/bin/open-ssp-regtest'
    env = dict(os.environ, REGTEST_DATA_DIR='/state/data', REGTEST_PROJECT='bundle-smoke',
               PATH='/runtime-bin', HOME='/state/home', SPARK_ADMIN_TOKEN='regtest-spark-admin-token')
    Path(env['HOME']).mkdir(exist_ok=True)
    assert not Path(os.environ['REGTEST_VALIDATION_SOURCE_ROOT']).exists()
    for name in ('git', 'cargo', 'go', 'rustc', 'gcc', 'cc'):
        assert shutil.which(name, path=env['PATH']) is None

    def cli(*args, check=True):
        print('+ open-ssp-regtest', *args, flush=True)
        result = subprocess.run([runner, *args], env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=900)
        print(result.stdout, flush=True)
        if check and result.returncode:
            raise RuntimeError(f'{args} failed ({result.returncode})')
        return result

    def get(url):
        with urllib.request.urlopen(url, timeout=20) as response:
            return json.load(response)

    def height():
        # Esplora can still be indexing old blocks after mining has stopped.
        # Use the authoritative tip, also exercising the bundled Bitcoin CLI.
        return int(subprocess.check_output([
            '/opt/fixture/bin/bitcoin-cli', '-regtest', '-rpcconnect=127.0.0.1',
            '-rpcport=8332', '-rpcuser=testutil', '-rpcpassword=testutilpassword',
            'getblockcount'], env=env, text=True, timeout=20))

    def collect(label):
        destination = Path('/state/logs') / label
        destination.mkdir(parents=True, exist_ok=True)
        for log in Path('/state/data/bundle-smoke/native').glob('*.log'):
            shutil.copy2(log, destination / log.name)

    manifest = json.loads(Path('/opt/fixture/manifest.json').read_text())
    for name, expected in manifest['files'].items():
        assert hashlib.sha256((Path('/opt/fixture') / name).read_bytes()).hexdigest() == expected, name
    info = json.loads(cli('info').stdout)
    assert info['bundle_root'] == '/opt/fixture'
    assert info['data_dir'] == '/state/data/bundle-smoke/native'
    for cmd in ('init', 'build'):
        assert cli(cmd, check=False).returncode != 0
    try:
        cli('up')
        # These reads must complete while fund holds the exclusive lifecycle lock.
        with concurrent.futures.ThreadPoolExecutor() as pool:
            funding = pool.submit(cli, 'fund', 'a', '5000000')
            time.sleep(0.2)
            for side in ('a', 'b'):
                cli('ldk', side, 'get-node-info')
            funding.result()
        cli('ldk', 'a', 'list-channels')
        cli('ldk', 'b', 'bolt11-receive', '500sat', '-d', 'bundle-smoke')
        status = urllib.request.Request('http://127.0.0.1:5000/status',
                                        headers={'Authorization': 'Bearer regtest-spark-admin-token'})
        assert get(status)['spark']['available_sats'] >= 5_000_000
        cli('miner', 'stop')
        time.sleep(2)
        before = height()
        time.sleep(12)
        assert height() == before, 'automatic mining did not stop'
        deadline = time.monotonic() + 60
        while get('http://127.0.0.1:30000/blocks/tip/height') != before:
            assert time.monotonic() < deadline, 'Esplora did not catch up'
            time.sleep(1)
        cli('miner', 'start')
        deadline = time.monotonic() + 30
        while height() <= before:
            assert time.monotonic() < deadline, 'automatic mining did not resume'
            time.sleep(1)
        cli('certs', '/state/copied-certs')
        for i in range(3):
            assert (Path('/state/copied-certs') / f'server_{i}.crt').read_text().startswith('-----BEGIN CERTIFICATE-----')
        cli('status')
        assert '--- electrs ---' in cli('logs', 'ssp', 'electrs').stdout
        cli('stop')
        cli('start')  # Includes the Esplora TIME_WAIT regression.
        cli('ldk', 'b', 'get-node-info')
        collect('first')
        cli('reset')
        assert not Path(info['data_dir']).exists()
        assert Path('/state/data/native.lock').exists()
        cli('up')  # A consecutive fresh fixture, with regenerated TLS/databases.
        cli('status')
        collect('second')
        cli('reset')
        assert not Path(info['data_dir']).exists()
        print('PASS: offline read-only bundle startup, 5M funding, concurrent LDK, mining, TLS, logs, resume and consecutive reset/up', flush=True)
    finally:
        collect('final')
        cli('reset', check=False)


def main():
    if sys.argv[1:] == ['--inner']:
        inner()
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--pg-bin', type=Path, default=None)
    args = parser.parse_args()
    archive = args.archive.resolve()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    extract = work / 'extracted'
    extract.mkdir(exist_ok=True)
    with tarfile.open(archive) as tar:
        tar.extractall(extract, filter='data')
    bundle = extract / archive.name.removesuffix('.tar.gz')
    state = work / 'state'
    state.mkdir(exist_ok=True)
    pg = args.pg_bin or Path(os.environ.get('PGBIN') or subprocess.check_output(['pg_config', '--bindir'], text=True).strip())
    command = ['bwrap', '--unshare-pid', '--unshare-net', '--die-with-parent',
               '--ro-bind', '/usr', '/usr', '--ro-bind', '/etc', '/etc',
               '--symlink', 'usr/bin', '/bin', '--symlink', 'usr/lib', '/lib',
               '--symlink', 'usr/lib64', '/lib64', '--proc', '/proc', '--dev', '/dev',
               '--tmpfs', '/tmp', '--dir', '/home', '--dir', '/opt', '--dir', '/runtime-bin',
               '--ro-bind', shutil.which('openssl'), '/runtime-bin/openssl',
               '--ro-bind', str(bundle), '/opt/fixture', '--bind', str(state), '/state',
               '--ro-bind', str(Path(__file__).resolve()), '/validate.py',
               '--setenv', 'PGBIN', str(pg),
               '--setenv', 'REGTEST_VALIDATION_SOURCE_ROOT', str(Path(__file__).resolve().parents[2]),
               '--chdir', '/state']
    # Hide build tools even at their usual absolute paths.
    for name in ('git', 'cargo', 'rustc', 'gcc', 'g++', 'cc', 'c++', 'clang', 'go', 'make'):
        path = Path('/usr/bin') / name
        if path.exists():
            command += ['--ro-bind', '/dev/null', str(path.resolve())]
    command += ['/usr/bin/python3', '/validate.py', '--inner']
    subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
