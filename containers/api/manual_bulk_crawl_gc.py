"""Garbage collection for crawl requests: expired past grace, or cancelled and drained."""
import logging
import signal
from threading import Event

import click
from libs.obslog import configure

from .config_loader import api_config_path, load_api_config
from .crawl_requests import CrawlRequestService


@click.command()
@click.option("--config", "config_path", default=None, type=click.Path(exists=True, dir_okay=False))
@click.option("--once", is_flag=True, help="Collect once, then exit.")
def main(config_path, once):
    config = load_api_config(api_config_path(config_path))
    crawl_config = config["crawl_requests"]
    interval = crawl_config.get("gc_interval_sec", 3600)
    if isinstance(interval, bool) or not isinstance(interval, int) or interval < 1:
        raise click.ClickException("crawl_requests.gc_interval_sec must be a positive integer")
    configure(service="manual_bulk_crawl_gc")
    logger = logging.getLogger(__name__)
    service = CrawlRequestService(crawl_config)
    stopped = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopped.set())
    while not stopped.is_set():
        try:
            removed = service.collect_garbage()
            logger.info("crawl_requests.gc_completed", extra={"removed_requests": removed})
        except OSError:
            logger.exception("crawl_requests.gc_failed")
            if once:
                raise
        if once:
            return
        stopped.wait(interval)


if __name__ == "__main__":
    main()
