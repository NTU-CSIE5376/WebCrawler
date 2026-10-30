"""Garbage collection for jobs submitted to the manual bulk crawl API."""
import logging
import signal
from threading import Event

import click

from libs.obslog import configure
from .config_loader import api_config_path, load_api_config
from .service import ManualBulkCrawlService


@click.command()
@click.option("--config", "config_path", default=None, type=click.Path(exists=True, dir_okay=False))
@click.option("--once", is_flag=True, help="Collect expired jobs once, then exit.")
def main(config_path, once):
    config = load_api_config(api_config_path(config_path))
    crawl_config = config["manual_bulk_crawl"]
    interval = crawl_config.get("gc_interval_sec", 3600)
    if isinstance(interval, bool) or not isinstance(interval, int) or interval < 1:
        raise click.ClickException("manual_bulk_crawl.gc_interval_sec must be a positive integer")
    configure(service="manual_bulk_crawl_gc")
    logger = logging.getLogger(__name__)
    service = ManualBulkCrawlService(crawl_config, route_urls=None)
    stopped = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stopped.set())
    while not stopped.is_set():
        try:
            removed = service.collect_expired()
            logger.info("manual_bulk_crawl.gc_completed", extra={"removed_jobs": removed})
        except OSError:
            logger.exception("manual_bulk_crawl.gc_failed")
            if once:
                raise
        if once:
            return
        stopped.wait(interval)


if __name__ == "__main__":
    main()
