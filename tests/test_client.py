import pytest

from sfapi_client import Client, SfApiError


@pytest.mark.public
def test_no_creds():
    with Client() as client:
        assert client is not None


@pytest.mark.public
def test_no_creds_auth_required(test_machine):
    with Client() as client:
        with pytest.raises(SfApiError):
            client.compute(test_machine)
