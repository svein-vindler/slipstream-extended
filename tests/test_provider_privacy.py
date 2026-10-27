"""New private account-binding metadata is forbidden even if force-added to Git."""

import pytest

from scripts.public_release_check import audit_paths


@pytest.mark.parametrize(
    "path",
    [
        ".garmin-provider.local.json",
        ".garmin-provider.local.json.lock",
        "nested/.garmin-provider.local.json.new-synthetic",
    ],
)
def test_account_binding_and_staging_files_cannot_be_published(path):
    assert audit_paths([path])
