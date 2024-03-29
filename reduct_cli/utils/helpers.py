"""Helper functions"""
import asyncio
import signal
import time
from asyncio import Semaphore, Queue
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict

from click import Abort
from reduct import EntryInfo, Bucket, Client
from rich.progress import Progress

from reduct_cli.config import read_config, Alias
from reduct_cli.utils.consoles import error_console
from reduct_cli.utils.humanize import pretty_size

signal_queue = Queue()


def get_alias(config_path: Path, name: str) -> Alias:
    """Helper method to parse alias from config"""
    conf = read_config(config_path)

    if name.split("/")[0] not in conf.aliases:
        error_console.print(f"Alias '{name}' doesn't exist")
        raise Abort()
    alias_: Alias = conf.aliases[name]
    return alias_


def build_client(config_path: Path, alias: str, timeout: float) -> Client:
    """Build client from alias"""
    alias_ = get_alias(config_path, alias)
    return Client(alias_.url, api_token=alias_.token, timeout=timeout)


def parse_path(path) -> Tuple[str, str]:
    """Parse path ALIAS/RESOURCE"""
    args = path.split("/")
    if len(args) != 2:
        raise RuntimeError(
            f"Path {path} has wrong format. It must be 'ALIAS/BUCKET_NAME'"
        )
    return tuple(args)


async def read_records_with_progress(
    entry: EntryInfo,
    bucket: Bucket,
    progress: Progress,
    sem: Semaphore,
    **kwargs,
):  # pylint: disable=too-many-locals
    """Read records from entry and show progress
    Args:
        entry (EntryInfo): Entry to read records from
        bucket (Bucket): Bucket to read records from
        progress (Progress): Progress bar to show progress
        sem (Semaphore): Semaphore to limit parallelism
    Keyword Args:
        start (Optional[datetime]): Start time point
        stop (Optional[datetime]): Stop time point
        timeout (int): Timeout for read operation
        parallel (int): Number of parallel tasks
    Yields:
        Record: Record from entry
    """

    def _to_timestamp(date: str) -> int:
        try:
            return int(date)
        except ValueError:
            return int(
                datetime.fromisoformat(date.replace("Z", "+00:00")).timestamp()
                * 1000_000
            )

    params = {
        "start": _to_timestamp(kwargs["start"])
        if kwargs["start"]
        else entry.oldest_record,
        "stop": _to_timestamp(kwargs["stop"])
        if kwargs["stop"]
        else entry.latest_record,
        "include": {},
        "exclude": {},
        "ttl": kwargs["timeout"] * kwargs["parallel"],
    }

    if "limit" in kwargs and kwargs["limit"]:
        params["limit"] = int(kwargs["limit"])

    params["include"] = extract_key_values(kwargs["include"])
    params["exclude"] = extract_key_values(kwargs["exclude"])
    last_time = params["start"]

    task = progress.add_task(
        f"Entry '{entry.name}' waiting", total=params["stop"] - params["start"]
    )
    async with sem:
        exported_size = 0
        count = 0
        stats = []
        speed = 0

        def stop_signal():
            signal_queue.put_nowait("stop")

        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, stop_signal)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, stop_signal)

        async for record in bucket.query(
            entry.name,
            **params,
        ):
            if signal_queue.qsize() > 0:
                # stop signal received
                progress.update(
                    task,
                    description=f"Entry '{entry.name}' "
                    f"(copied {count} records ({pretty_size(exported_size)}), stopped",
                    refresh=True,
                )
                return

            exported_size += record.size
            stats.append((record.size, time.time()))
            if len(stats) > 100:
                speed = sum(s[0] for s in stats) / (stats[-1][1] - stats[0][1])
                stats = stats[-50:]

            yield record

            progress.update(
                task,
                description=f"Entry '{entry.name}' "
                f"(copied {count} records ({pretty_size(exported_size)}), "
                f"speed {pretty_size(speed) if speed else '? B'}/s)",
                advance=record.timestamp - last_time,
                refresh=True,
            )
            last_time = record.timestamp
            count += 1

        progress.update(task, total=1, completed=True)


def filter_entries(entries: List[EntryInfo], names: List[str]) -> List[EntryInfo]:
    """Filter entries by names"""
    if not names or len(names) == 0:
        return entries

    if len(names) == 1 and names[0] == "":
        return entries

    def _filter(entry):
        for name in names:
            name = name.strip()
            if name == entry.name:
                return True
            if name.endswith("*") and entry.name.startswith(name[:-1]):
                return True
        return False

    return list(filter(_filter, entries))


def extract_key_values(items: List[str]) -> Dict[str, str]:
    """Extract key-values from list of items"""
    params = {}
    for item in items:
        if item:
            key, value = item.split("=")
            params[key] = value

    return params
