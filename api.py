from datetime import datetime
from fastapi import FastAPI, Depends, Query, HTTPException
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Float, DateTime, select, inspect, text
from typing import List, Optional, Any, Dict


# Database configuration
with open('config.json', 'r') as f:
    DATABASE_CONFIG = json.load(f)

DATABASE_URL = f"postgresql+asyncpg://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@" \
           f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# Data table model definition
Base = declarative_base()


class WindData(Base):
    __tablename__ = "data"

    timestamp = Column(DateTime(timezone=True), primary_key=True)
    wind_speed = Column(Float)
    power = Column(Float)
    ambient_temperature = Column(Float)

# Helper to validate column names securely
VALID_COLUMNS = [c.key for c in inspect(WindData).mapper.column_attrs]

app = FastAPI(title="Wind Data API")


# Dependency injection
async def get_db():
    async with SessionLocal() as session:
        yield session


@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    try:
        # Test the DB connection with simple query
        await db.execute(text("SELECT 1"))
        return {
            "status": "online",
            "database": "connected",
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(
            status_code=503, 
            detail=f"Database connection failed: {str(e)}"
        )


@app.get("/data", response_model=List[Dict[str, Any]])
async def get_sensor_data(
    start_time: Optional[datetime] = Query(None, description="Filter start time (ISO 8601)"),
    end_time: Optional[datetime] = Query(None, description="Filter end time (ISO 8601)"),
    # Allow selecting multiple columns: /data?cols=power&cols=wind_speed
    cols: List[str] = Query(
        default=["timestamp", "wind_speed", "power", "ambient_temperature"], 
        description="List of columns to return"
    ),
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch data filtering by time and selecting specific columns.
    """
    # Validate requested columns to prevent 'Internal Server Errors'
    selected_attributes = []
    for col in cols:
        if col not in VALID_COLUMNS:
            raise HTTPException(status_code=400, detail=f"Invalid column: {col}")
        selected_attributes.append(getattr(WindData, col))

    stmt = select(*selected_attributes) #select only requested columns

    # Apply time filters if provided
    if start_time:
        stmt = stmt.where(WindData.timestamp >= start_time)
    if end_time:
        stmt = stmt.where(WindData.timestamp <= end_time)

    # Execute
    result = await db.execute(stmt)
    
    # Transform result to list of dicts
    data = []
    for row in result.all():
        # row._mapping converts the result row into a dictionary-like object
        data.append(dict(row._mapping))

    return data