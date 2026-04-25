from __future__ import annotations
import time
import argparse
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.folder_reader import Progress, FolderReader
from libs.obslog import configure as configure_logging

from .service import RouterService, load_router_config

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--router-id", type=int, required=True)
    args = ap.parse_args()

    router_id = args.router_id
    configure_logging(service="router", worker_id=router_id)
    cfg = load_router_config(args.config, router_id)
    svc = RouterService(cfg)

    prog = Progress(cfg.progress_template.format(id=router_id))

    reader = FolderReader(
        cfg.crawler_dir_template.format(id=router_id),
        prog,
        cfg.interval_minutes
    )

    while True:
        progressed = False
        for date, time_, folder in reader.iter_ready_folders():
            svc.process_folder(folder)
            prog.advance(date, time_)
            progressed = True
        if not progressed:
            time.sleep(cfg.interval_minutes * 60)


if __name__ == "__main__":
    main()

