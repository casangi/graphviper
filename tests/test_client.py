import os
import re
import pathlib
import graphviper

from graphviper.dask.client import local_client


class TestGraphViperClient:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given test class
        such as fetching test data"""
        pass

    @classmethod
    def teardown_class(cls):
        """teardown any state that was previously setup with a call to setup_class
        such as deleting test data"""
        pass

    def setup_method(self):
        """setup any state specific to all methods of the given class"""
        pass

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        pass

    def test_client_spawn(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB to create an instance of the
        astrohack Dask client.
        """

        log_params = {"log_level": "DEBUG"}

        client = local_client(cores=2, memory_limit="8GB", log_params=log_params)

        try:
            if graphviper.dask.client._current_client.get() is None:
                raise OSError

        except OSError:
            assert False

        finally:
            client.shutdown()

    def test_client_dask_dir(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB to create an instance of the
        astrohack Dask client. Check that temporary files are written to dask_local_dir.
        """

        try:
            log_params = {"log_level": "DEBUG"}

            path = pathlib.Path(".").cwd() / "dask_test_dir"
            client = local_client(
                cores=2,
                memory_limit="8GB",
                log_params=log_params,
                dask_local_dir=str(path),
            )

            if path.exists() is False:
                raise FileNotFoundError

        except FileNotFoundError:
            assert False

        finally:
            client.shutdown()

    def test_client_logger(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB without any errors and the messages
        will be logged in the terminal.
        """

        log_params = {
            "log_level": "DEBUG",
            "log_to_file": True,
            "log_file": "graphviper_log_file",
        }

        client = local_client(
            cores=2,
            memory_limit="8GB",
            log_params=log_params
        )

        files = os.listdir(".")

        try:
            for file in files:
                if re.match("^graphviper_log_file+[0-9].*log", file) is not None:
                    return

            raise FileNotFoundError

        except FileNotFoundError:
            assert False

        finally:
            client.shutdown()
