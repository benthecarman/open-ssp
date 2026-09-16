#!/usr/bin/env python3
"""Build a self-contained native fixture archive. Run from any working directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def run(*args, cwd=ROOT, **kwargs):
    return subprocess.check_output(args, cwd=cwd, text=True, **kwargs).strip()


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True, help='vMAJOR.MINOR.PATCH[-prerelease]')
    parser.add_argument('--output', type=Path, default=ROOT / 'dist')
    args = parser.parse_args()
    if not re.fullmatch(r'v\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?', args.version):
        parser.error('version must be vMAJOR.MINOR.PATCH[-prerelease]')
    if (platform.system(), platform.machine()) != ('Linux', 'x86_64'):
        parser.error('only Linux x86_64 builds are supported')
    # Only the pinned submodules are supported in published bundles.
    for key in ('SPARK_REF', 'LDK_SERVER_REF', 'SPARK_OPERATOR_COMMIT', 'CARGO_TARGET_DIR'):
        if key in os.environ:
            parser.error(f'unset {key} before packaging')
    env = dict(os.environ, CARGO_PROFILE_DEV_DEBUG='0')
    subprocess.run(['cargo', 'build', '--locked', '--manifest-path', 'e2e/breez/Cargo.toml', '--bin', 'open-ssp-breez-e2e'],
                   cwd=ROOT, env=env, check=True)
    runner = ROOT / 'e2e/breez/target/debug/open-ssp-breez-e2e'
    subprocess.run([runner, 'build'], cwd=ROOT, env=env, check=True)
    revisions = {name: run('git', 'rev-parse', 'HEAD', cwd=ROOT / path)
                 for name, path in [('open-ssp', '.'), ('spark', 'vendor/spark'),
                                    ('ldk-server', 'vendor/ldk-server'), ('breez-sdk', 'vendor/breez-sdk')]}
    dirty = bool(run('git', 'status', '--porcelain', '--untracked-files=no'))
    for component in ('spark', 'ldk-server', 'breez-sdk'):
        path = ROOT / 'vendor' / component
        if run('git', 'status', '--porcelain', '--untracked-files=no', cwd=path):
            raise SystemExit(f'{component} has local changes; commit them before packaging')
    tool_source = (ROOT / 'e2e/breez/src/native/tools.rs').read_text()
    electrs = re.search(r'const ELECTRS_REVISION: &str = "([a-f0-9]+)"', tool_source)[1]
    revisions['electrs'] = electrs
    spark = ROOT / '.regtest/native-sources' / f'spark-{revisions["spark"]}'
    name = f'open-ssp-regtest-{args.version}-linux-x86_64'
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as temp:
        bundle = Path(temp) / name
        (bundle / 'bin').mkdir(parents=True)
        binaries = {
            'open-ssp-regtest': runner,
            'open-ssp': ROOT / 'target/debug/open-ssp',
            'spark-operator': ROOT / '.regtest/native-tools/spark-operator',
            'spark-frost-signer': ROOT / '.regtest/native-build/signer/debug/spark-frost-signer',
            'ldk-server': ROOT / '.regtest/native-build/ldk/debug/ldk-server',
            'ldk-server-cli': ROOT / '.regtest/native-build/ldk/debug/ldk-server-cli',
            'electrs': ROOT / '.regtest/native-tools' / f'electrs-{electrs}',
            'atlas': ROOT / '.regtest/native-tools/atlas',
            'bitcoind': ROOT / '.regtest/native-tools/bitcoin-29.0/bin/bitcoind',
            'bitcoin-cli': ROOT / '.regtest/native-tools/bitcoin-29.0/bin/bitcoin-cli',
        }
        linkage = {}
        for binary, source in binaries.items():
            target = bundle / 'bin' / binary
            shutil.copy2(source, target)
            subprocess.run(['strip', '--strip-debug', target], check=True)
            target.chmod(0o755)
            result = subprocess.run(['ldd', target], capture_output=True, text=True)
            linkage[binary] = result.stdout.strip() or result.stderr.strip()
            if 'not found' in linkage[binary]:
                raise SystemExit(f'missing shared library for {binary}: {linkage[binary]}')
        upstream = bundle / 'share/upstream'
        upstream.mkdir(parents=True)
        for asset in ['bitcoin.conf', 'config.json', 'operator.config.yaml',
                      'operator_0.key', 'operator_1.key', 'operator_2.key']:
            shutil.copy2(ROOT / 'e2e/upstream' / asset, upstream / asset)
        for schema in ('ent', 'entephemeral'):
            path = Path(f'spark/so/{schema}/migrate/migrations')
            shutil.copytree(spark / path, bundle / 'share/spark' / path)
        licenses = bundle / 'share/licenses'
        licenses.mkdir()
        for component, path in [('open-ssp', ROOT), ('spark', spark),
                                ('ldk-server', ROOT / 'vendor/ldk-server'),
                                ('breez-sdk', ROOT / 'vendor/breez-sdk'),
                                ('electrs', ROOT / '.regtest/native-sources' / f'electrs-{electrs}')]:
            for source in path.glob('LICENSE*'):
                shutil.copy2(source, licenses / f'{component}-{source.name}')
        for license_file in (ROOT / 'scripts/regtest/licenses').iterdir():
            shutil.copy2(license_file, licenses / license_file.name)
        shutil.copy2(ROOT / 'docs/REGTEST_BUNDLE.md', bundle / 'README.md')
        manifest = {
            'schema_version': 1, 'version': args.version, 'target': 'linux-x86_64',
            'components': revisions, 'source_dirty': dirty,
            'tools': {'bitcoin-core': {'version': '29.0', 'archive_sha256':
                      'a681e4f6ce524c338a105f214613605bac6c33d58c31dc5135bbc02bc458bb6c'},
                      'atlas-community': {'version': '1.0.0', 'download_sha256':
                      '9933f9a75cad6962ba0cf39813ecc2b1454aa35e952e4bcc36ee714c921ac860'}},
            'build': {'rustc': run('rustc', '--version'), 'go': run(shutil.which('go') or '/usr/local/go/bin/go', 'version'),
                      'libc': platform.libc_ver(), 'os': platform.freedesktop_os_release(),
                      'profiles': 'Electrs release; signer dev opt-level=1; other Rust dev; stripped debug info',
                      'spark_overlay': 'four operator listeners bound to 127.0.0.1'},
            'runtime_dependencies': ['PostgreSQL server tools (initdb, postgres, psql)',
                                     'OpenSSL command line', 'libzmq5 and its dependencies', 'shared libraries listed in linkage'],
            'linkage': linkage,
            'files': {str(p.relative_to(bundle)): digest(p) for p in sorted(bundle.rglob('*')) if p.is_file()},
        }
        manifest_path = bundle / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
        # The archive's checksum covers the manifest; the manifest hashes every payload file.
        archive = output / f'{name}.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            tar.add(bundle, arcname=name)
        sidecar = output / f'{name}.manifest.json'
        shutil.copy2(manifest_path, sidecar)
        (output / f'{name}.sha256').write_text(''.join(
            f'{digest(p)}  {p.name}\n' for p in (archive, sidecar)))
        print(archive)


if __name__ == '__main__':
    main()
