from ..libs.worker import run
from .service import DiscoveryIngestor

if __name__ == "__main__":
    run("discovery_ingestor", DiscoveryIngestor)
