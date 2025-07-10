from sqlalchemy import Column, Integer, String, BigInteger, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Base class for all models
Base = declarative_base()

# Model for datoms_TBL
class Datoms(Base):
    __tablename__ = "datoms_TBL"

    id = Column(String(100), primary_key=True)
    category = Column(String(50))
    event_time = Column(BigInteger)  # UNIX timestamp
    generated_at = Column(BigInteger)
    rule_template_name = Column(String(100))
    message = Column(String)  # NVARCHAR(MAX) -> String with no length restriction
    rule_param = Column(String(100))
    param_threshold = Column(String(100), nullable=True)
    param_value = Column(Integer)
    entity_type = Column(String(50))
    entity_id = Column(Integer)

# Model for thingsup_TBL
class ThingsUp(Base):
    __tablename__ = "thingsup_TBL"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_id = Column(Integer)

    device_id = Column(Integer)
    device_uniqueid = Column(String(100))
    device_name = Column(String(100))
    device_time = Column(DateTime)
    location_lat = Column(Float)
    location_long = Column(Float)

    alert_name = Column(String(100))
    alert_raised_at = Column(DateTime)
    alert_cleared_at = Column(DateTime, nullable=True)
    alert_checked_at = Column(DateTime)

    event_name = Column(String(100))
    event_id = Column(Integer)
    event_value = Column(Float)
    event_raised = Column(Boolean)
    event_raised_at = Column(DateTime)
    event_cleared_at = Column(DateTime)
    event_checked_at = Column(DateTime)
    event_type = Column(String(50))
