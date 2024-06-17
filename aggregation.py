import asyncio
import json
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from bson import json_util

MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "RLTDatabase"
COLLECTION_NAME = "salaryCollection"

async def aggregate_salaries(dt_from, dt_upto, group_type):
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    
    if group_type == 'hour':
        group_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        group_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'month':
        group_format = "%Y-%m-01T00:00:00"
    else:
        raise ValueError("Invalid group_type. Must be one of: 'hour', 'day', 'month'")
    
    pipeline = [
        {"$match": {"dt": {"$gte": dt_from, "$lt": dt_upto}}},
        {"$group": {"_id": {"$dateToString": {"format": group_format, "date": "$dt"}}, "total": {"$sum": "$value"}}},
        {"$sort": {"_id": ASCENDING}}
    ]
    
    result = await collection.aggregate(pipeline).to_list(None)
    
    dataset = [item['total'] for item in result]
    labels = [item['_id'] for item in result]

    
    return {"dataset": dataset, "labels": labels}

# Example usage:
# asyncio.run(aggregate_salaries("2022-09-01T00:00:00", "2022-12-31T23:59:00", "month"))