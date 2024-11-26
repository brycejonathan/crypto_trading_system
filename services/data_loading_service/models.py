# data_loading_service/models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    ForeignKey,
    UniqueConstraint,
    Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class DimDate(Base):
    """
    Dimension table for dates.
    """
    __tablename__ = 'dim_date'

    date_id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, unique=True)
    month = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)

    # Relationships
    price_histories = relationship('FactPriceHistory', back_populates='date')

    def __init__(self, date):
        self.date = date
        self.month = date.month
        self.quarter = (date.month - 1) // 3 + 1
        self.year = date.year

    def __repr__(self):
        return f"<DimDate(date_id={self.date_id}, date={self.date})>"


class DimCrypto(Base):
    """
    Dimension table for cryptocurrencies.
    """
    __tablename__ = 'dim_crypto'

    crypto_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    symbol = Column(String(20), nullable=False, unique=True)
    market_cap = Column(Float)

    # Relationships
    price_histories = relationship('FactPriceHistory', back_populates='crypto')

    def __init__(self, name, symbol, market_cap=None):
        self.name = name
        self.symbol = symbol
        self.market_cap = market_cap

    def __repr__(self):
        return f"<DimCrypto(crypto_id={self.crypto_id}, symbol={self.symbol})>"


class FactPriceHistory(Base):
    """
    Fact table for price history.
    """
    __tablename__ = 'fact_price_history'
    __table_args__ = (
        UniqueConstraint('date_id', 'crypto_id', name='uix_date_crypto'),
        Index('idx_date_crypto', 'date_id', 'crypto_id')
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    date_id = Column(Integer, ForeignKey('dim_date.date_id'), nullable=False)
    crypto_id = Column(Integer, ForeignKey('dim_crypto.crypto_id'), nullable=False)
    open_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    volume = Column(Float)

    # Relationships
    date = relationship('DimDate', back_populates='price_histories')
    crypto = relationship('DimCrypto', back_populates='price_histories')

    def __init__(self, date_id, crypto_id, open_price, close_price, high_price, low_price, volume):
        self.date_id = date_id
        self.crypto_id = crypto_id
        self.open_price = open_price
        self.close_price = close_price
        self.high_price = high_price
        self.low_price = low_price
        self.volume = volume

    def __repr__(self):
        return f"<FactPriceHistory(date_id={self.date_id}, crypto_id={self.crypto_id})>"
