import os.path

import pytest

from memc_load import dot_rename, parse_appsinstalled


def test_valid_dot_rename():
    path = 'test.test'
    open(path, 'tw').close()
    dot_rename(path)
    assert os.path.exists('.test.test')
    os.remove('.test.test')


def test_valid_parse_installed(test_value):
    line, apps, def_id, dev_type, lat, lon = test_value
    app_installed = parse_appsinstalled(line)
    assert app_installed.apps == apps
    assert app_installed.def_id == def_id
    assert app_installed.dev_type == dev_type
    assert app_installed.lat == lat
    assert app_installed.lon == lon


def test_invalid_parse_installed(test_value):
    line, apps, def_id, dev_type, lat, lon = test_value
    app_installed = parse_appsinstalled(line)
    assert app_installed is None

