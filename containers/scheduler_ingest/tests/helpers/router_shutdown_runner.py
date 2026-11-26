"""Exercise the real router main loop and OS signals without a live database."""
from pathlib import Path
import sys
import time
from types import SimpleNamespace

from containers.scheduler_ingest.router import main as router_main


def run(root):
    class Engine:
        def dispose(self):
            (root / "disposed").touch()

    class Service:
        engine = Engine()

        def process_folder(self, folder):
            (root / "started").write_text(folder.name)
            while not (root / "finish").exists():
                time.sleep(.01)
            with (root / "output").open("a") as output:
                output.write(folder.name + "\n")

    cfg = SimpleNamespace(
        progress_template=str(root / "progress_{id:02d}.json"),
        crawler_dir_template=str(root / "crawler_{id:02d}"),
        interval_minutes=10,
    )
    router_main.configure_logging = lambda **kwargs: None
    router_main.load_router_config = lambda *args: cfg
    router_main.RouterService = lambda cfg: Service()
    sys.argv = ["router", "--config", "unused", "--router-id", "0"]
    router_main.main()


if __name__ == "__main__":
    run(Path(sys.argv[1]))
