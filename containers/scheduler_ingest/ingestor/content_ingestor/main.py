from ..libs.worker import run
from .service import ContentIngestor

if __name__ == "__main__":
    run("content_ingestor", ContentIngestor)
