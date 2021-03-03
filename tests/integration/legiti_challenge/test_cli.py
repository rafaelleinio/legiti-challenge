import shutil

import pytest
from click.testing import CliRunner

from legiti_challenge.cli import cli


class TestCli:
    @pytest.mark.parametrize(
        "args",
        [
            "",
            "--help",
            "list-pipelines",
            "describe feature_store.user_orders",
            "execute feature_store.user_orders --end-date 2020-07-17",
            "execute feature_store.user_chargebacks --end-date 2020-07-17",
            "execute dataset.awesome_dataset",
        ],
    )
    def test_cli(self, args):
        """Simple test for entrypoint."""
        # arrange
        runner = CliRunner()

        # act
        result = runner.invoke(cli, args)

        # assert
        assert result.exit_code == 0

    def test_tear_down(self):
        # removing created data
        shutil.rmtree("data/feature_store", ignore_errors=True)
        shutil.rmtree("data/datasets", ignore_errors=True)
