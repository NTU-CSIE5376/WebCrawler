from sqlalchemy import Column, Integer, String, Float
from libs.db.base import Base

class DomainState(Base):
    __tablename__ = "domain_state"

    domain_id = Column(Integer, primary_key=True)
    domain = Column(String, nullable=False, unique=True)

    shard_id = Column(Integer, nullable=False)

    # Crawl priority signal
    domain_score = Column(Float, default=0.0)

