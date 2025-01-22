from sqlalchemy import Boolean, Column, Date, Numeric, String, Time

from jqdata_fetcher.db.db import Base


class FuturesInfo(Base):
    __tablename__ = 'futures_info'
    code = Column(String, primary_key=True)
    display_name = Column(String)
    name = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)


class FuturesDailyBar(Base):
    __tablename__ = 'futures_daily_bar'
    code = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(Numeric)
    money = Column(Numeric)
    paused = Column(Boolean)
    high_limit = Column(Numeric)
    low_limit = Column(Numeric)
    avg = Column(Numeric)
    pre_close = Column(Numeric)
    open_interest = Column(Numeric)


class FuturesMinutelyBar(Base):
    __tablename__ = 'futures_minutely_bar'
    code = Column(String, primary_key=True)
    time = Column(Time, primary_key=True)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(Numeric)
    money = Column(Numeric)
    open_interest = Column(Numeric)


class FuturesContinuousContract(Base):
    __tablename__ = 'futures_continuous_contract'
    date = Column(Date, primary_key=True)
    continuous_code = Column(String, primary_key=True)
    contract_code = Column(String)
