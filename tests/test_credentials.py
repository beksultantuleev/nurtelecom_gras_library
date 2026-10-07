"""Tests for VSO credential reading and Easy Connect DSN connections (no DB/Vault)."""
import os

import pytest

from nurtelecom_gras_library import (
    OracleDataRetriever,
    OracleGeoDataImporter,
    get_all_cred_dict_vso,
    get_db_connection,
    parse_easy_connect_dsn,
)


def _write_secret_dir(path, items):
    """Mimic a K8s Secret volume: ..data dir + per-key symlinks."""
    data_dir = path / '..2026_09_30_00_00_00.000000000'
    data_dir.mkdir(parents=True)
    for key, value in items.items():
        (data_dir / key).write_text(value)
    os.symlink(data_dir.name, path / '..data')
    for key in items:
        os.symlink(os.path.join('..data', key), path / key)
    return path


# --- get_all_cred_dict_vso -------------------------------------------------

def test_vso_merges_multiple_dirs(tmp_path):
    main = _write_secret_dir(tmp_path / 'main', {
        'DWH_DSN': '10.0.0.1:1521/DWH\n',
        '_raw': '{"DWH_DSN": "..."}',
    })
    db = _write_secret_dir(tmp_path / 'db', {'USER_DWH': 'secret'})

    creds = get_all_cred_dict_vso([str(main), str(db)])

    assert creds == {'DWH_DSN': '10.0.0.1:1521/DWH', 'USER_DWH': 'secret'}


def test_vso_single_path_string(tmp_path):
    d = _write_secret_dir(tmp_path / 'main', {'A': '1'})
    assert get_all_cred_dict_vso(str(d)) == {'A': '1'}


def test_vso_paths_from_env(tmp_path, monkeypatch):
    a = _write_secret_dir(tmp_path / 'a', {'A': '1'})
    b = _write_secret_dir(tmp_path / 'b', {'B': '2'})
    monkeypatch.setenv('VSO_SECRET_PATHS', f'{a},{b}')
    assert get_all_cred_dict_vso() == {'A': '1', 'B': '2'}


def test_vso_duplicate_key_raises(tmp_path):
    a = _write_secret_dir(tmp_path / 'a', {'KEY': '1'})
    b = _write_secret_dir(tmp_path / 'b', {'KEY': '2'})
    with pytest.raises(ValueError, match='KEY') as exc:
        get_all_cred_dict_vso([str(a), str(b)])
    assert '1' not in str(exc.value).replace(str(tmp_path), '')


def test_vso_no_paths_raises(monkeypatch):
    monkeypatch.delenv('VSO_SECRET_PATHS', raising=False)
    with pytest.raises(ValueError):
        get_all_cred_dict_vso()


def test_vso_missing_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        get_all_cred_dict_vso(str(tmp_path / 'nope'))


# --- parse_easy_connect_dsn ------------------------------------------------

@pytest.mark.parametrize('dsn, expected', [
    ('10.0.0.1:1521/DWH', ('10.0.0.1', '1521', 'DWH')),
    ('  db.host:1600/SVC.DOMAIN ', ('db.host', '1600', 'SVC.DOMAIN')),
    ('//10.0.0.1:1521/DWH', ('10.0.0.1', '1521', 'DWH')),
    ('10.0.0.1/DWH', ('10.0.0.1', '1521', 'DWH')),
])
def test_parse_dsn_valid(dsn, expected):
    assert parse_easy_connect_dsn(dsn) == expected


@pytest.mark.parametrize('dsn', ['', '10.0.0.1:1521', '10.0.0.1:abc/DWH', None])
def test_parse_dsn_invalid(dsn):
    with pytest.raises(ValueError):
        parse_easy_connect_dsn(dsn)


# --- OracleDataRetriever with dsn ------------------------------------------

def test_dsn_connector_matches_host_connector():
    by_host = OracleDataRetriever('u', 'p@ss', '10.0.0.1', '1521', 'DWH')
    by_dsn = OracleDataRetriever('u', 'p@ss', dsn='10.0.0.1:1521/DWH')
    assert by_dsn.dsn == by_host.dsn
    assert by_dsn.engine_url == by_host.engine_url
    assert (by_dsn.host, by_dsn.port, by_dsn.service_name) == ('10.0.0.1', '1521', 'DWH')
    assert by_dsn.easy_connect_dsn == '10.0.0.1:1521/DWH'


def test_from_dsn_classmethod_keeps_subclass():
    db = OracleGeoDataImporter.from_dsn('u', 'p', '10.0.0.1:1521/DWH')
    assert isinstance(db, OracleGeoDataImporter)
    assert db.service_name == 'DWH'


def test_connector_requires_host_or_dsn():
    with pytest.raises(ValueError):
        OracleDataRetriever('u', 'p')


# --- get_db_connection -----------------------------------------------------

def test_get_db_connection_prefers_dsn_key():
    creds = {'U_DWH': 'p', 'DWH_DSN': '10.0.0.2:1600/SVC',
             'DWH_IP': 'ignored', 'DWH_PORT': '1', 'DWH_SERVICE_NAME': 'X'}
    db = get_db_connection('u', 'dwh', creds)
    assert (db.host, db.port, db.service_name) == ('10.0.0.2', '1600', 'SVC')


def test_get_db_connection_legacy_keys():
    creds = {'U_DWH': 'p', 'DWH_IP': '10.0.0.3',
             'DWH_PORT': '1521', 'DWH_SERVICE_NAME': 'DWH'}
    db = get_db_connection('u', 'dwh', creds)
    assert type(db) is OracleDataRetriever
    assert db.host == '10.0.0.3'
    assert db.easy_connect_dsn is None


def test_get_db_connection_missing_key():
    with pytest.raises(ValueError, match='DWH_IP'):
        get_db_connection('u', 'dwh', {'U_DWH': 'p'})


def test_get_db_connection_geodata_with_dsn():
    db = get_db_connection('u', 'dwh', {'U_DWH': 'p', 'DWH_DSN': 'h:1521/DWH'},
                           geodata=True)
    assert isinstance(db, OracleGeoDataImporter)


def test_get_db_connection_from_vso_paths(tmp_path):
    main = _write_secret_dir(tmp_path / 'main', {'DWH_DSN': '10.0.0.5:1521/DWH'})
    users = _write_secret_dir(tmp_path / 'db', {'U_DWH': 'p'})
    db = get_db_connection('u', 'dwh', vso_paths=[str(main), str(users)])
    assert db.host == '10.0.0.5'
    assert db.password == 'p'
