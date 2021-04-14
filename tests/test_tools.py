import pytest
from unittest.mock import patch
import sys
import subprocess
import concurrent.futures
import signal
from .test_common import retry

from asyncua.tools import uaread, uals, uawrite, uahistoryread, uaclient, uadiscover, uacall

pytestmark = pytest.mark.asyncio

ROOT_NODE = "i=85"
RW_NODE = "i=3078"


@retry(times=5, sleep=1, exceptions=[AssertionError])
def run_in_executor(executor, tool):
    """
    retry as the server running in a different thread
    might not be ready to accept the first connections
    """
    result = executor.submit(tool)
    future = next(concurrent.futures.as_completed([result]))
    str_excp = repr(future.exception())
    assert str_excp == "SystemExit(0)"


async def test_cli_tools(running_server):
    # admin privileges are only needed for uawrite
    url = running_server.replace("//", "//admin@")
    default_opts = ["mock_func", "-u", f"{url}"]
    rw_node = ["-n", RW_NODE]
    call_node = ["-n", ROOT_NODE]
    write_val = ["val_to_write"]
    rw_opts = default_opts + rw_node
    write_opts = rw_opts + write_val
    call_opts = default_opts + call_node + write_val

    tool_opts = {}
    tool_opts[uaread] = rw_opts
    tool_opts[uals] = rw_opts
    tool_opts[uawrite] = write_opts
    tool_opts[uahistoryread] = rw_opts
    tool_opts[uaclient] = default_opts
    tool_opts[uadiscover] = default_opts
    tool_opts[uacall] = call_opts

    for tool in (uaread, uals, uawrite, uahistoryread, uaclient, uadiscover, uacall):
        # It's necessary to mock argv, else the tool is invoked with *pytest's* argv
        with patch.object(sys, 'argv', tool_opts[tool]):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                run_in_executor(executor, tool)


async def test_cli_tools_which_require_sigint(running_server):
    url = running_server
    tools = (
        ["tools/uaserver"],
        ["tools/uasubscribe", "-u", url, "-n", RW_NODE]
    )
    for tool in tools:
        proc = subprocess.Popen(tool, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with pytest.raises(subprocess.TimeoutExpired):
            # we consider there's no error if the process is still alive
            proc.communicate(timeout=2)
        proc.send_signal(signal.SIGINT)
