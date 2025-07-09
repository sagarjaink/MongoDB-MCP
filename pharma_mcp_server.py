import os
import logging
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from fastmcp import FastMCP
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure

# --- Basic Setup ---
log = logging.getLogger("pharma_mcp")

# IMPORTANT: Get your connection string from MongoDB Atlas and paste it here.
# Replace <password> with your actual database user password.
CONNECTION_STRING = "mongodb+srv://sagarjaink:DYCPvmik49vs0e6G@test.2oxlhkm.mongodb.net/?retryWrites=true&w=majority&appName=Test"

mcp = FastMCP(
    "IMS Pharmaceutical Data Agent",
    instructions="Agent that can query a MongoDB database of IMS pharmaceutical market data. Focus on the most relevant fields: Dosage Form, NDC-TRIM, Corporation, Manufacturer, Brand/Generic, Rx Status, Strength, Pack Size, Pack Quantity, Combined Molecule, and March 2025 sales/units data.",
    dependencies=["pymongo"]
)

# --- The Tool Claude Will Call ---
@mcp.tool()
def execute_mongodb_query(
    query: Dict[str, Any],
    collection_name: str = "ims_may_2025",
    database_name: str = "pharma_data",
    projection: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    sort: Optional[List[tuple]] = None,
) -> List[Dict[str, Any]]:
    """
    Execute a MongoDB query on IMS pharmaceutical data and return the results.

    Args:
        query (Dict[str, Any]): The MongoDB query to execute.
        collection_name (str): The name of the collection to query.
        database_name (str): The name of the database.
        projection (Optional[Dict[str, Any]]): Fields to include/exclude in the results.
        limit (Optional[int]): Maximum number of results to return.
        sort (Optional[List[tuple]]): List of (key, direction) pairs for sorting.
    
    Returns:
        List[Dict[str, Any]]: The query results as a list of dictionaries.
    """
    client = None
    try:
        # Connect to MongoDB
        log.info(f"Connecting to MongoDB...")
        client = pymongo.MongoClient(CONNECTION_STRING)
        
        # Access the database
        db = client[database_name]
        
        # Access the collection
        collection = db[collection_name]
        
        # Execute the query with optional parameters
        log.info(f"Executing query on {database_name}.{collection_name}: {query}")
        cursor = collection.find(query, projection)
        
        # Apply sort if provided
        if sort:
            cursor = cursor.sort(sort)
            
        # Apply limit if provided
        if limit:
            cursor = cursor.limit(limit)
            
        # Convert cursor to list of dictionaries and handle ObjectID
        results = []
        for doc in cursor:
            doc['_id'] = str(doc['_id']) # Convert ObjectID to string for JSON serialization
            results.append(doc)
            
        return results

    except ConnectionFailure as e:
        log.error(f"MongoDB Connection Failure: {e}")
        raise Exception(f"Failed to connect to MongoDB: {str(e)}")
    except OperationFailure as e:
        log.error(f"MongoDB Operation Failure: {e}")
        raise Exception(f"MongoDB operation failed: {str(e)}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        raise Exception(f"An error occurred: {str(e)}")
    finally:
        # Close the connection
        if client:
            client.close()
            log.info("MongoDB connection closed.")


# --- A Resource to expose the schema to Claude ---
@mcp.resource("mongodb://pharma_data/ims_may_2025")
def get_pharma_schema() -> Dict[str, Any]:
    """Get the schema of the most relevant fields in the IMS pharmaceutical data collection."""
    return {
        "Dosage Form": {"type": "str", "description": "Product form (e.g., 'Capsule', 'Tablet', 'Tablet Extended Release')"},
        "NDC -TRIM": {"type": "int", "description": "National Drug Code identifiers (numeric)"},
        "Corporation": {"type": "str", "description": "Manufacturing corporation names"},
        "Manufacturer": {"type": "str", "description": "Manufacturer names"},
        "Brand/Generic": {"type": "str", "description": "Classification ('Brand', 'Generic', 'OTHER')"},
        "Rx Status": {"type": "str", "description": "Prescription status ('Rx', 'OTC')"},
        "Strength": {"type": "str", "description": "Drug strength/dosage (e.g., '600MG-800U', 'N/A')"},
        "Pack Size": {"type": "int", "description": "Package size (numeric, typically 1)"},
        "Pack Quantity": {"type": "int", "description": "Quantity per package (numeric)"},
        "Combined Molecule": {"type": "str", "description": "Active pharmaceutical ingredients"},
        "MAT  Mar 2025_Sales $": {"type": "str", "description": "Moving Annual Total sales (March 2025)"},
        "MAT  Mar 2025_Units": {"type": "int", "description": "Moving Annual Total units (March 2025)"},
        "MAT  Mar 2025_Eaches": {"type": "int", "description": "Moving Annual Total eaches (March 2025)"},
        "MAT  Mar 2025_NSP Ext. Units": {"type": "str", "description": "Moving Annual Total NSP Extended Units (March 2025)"}
    }

# --- Entrypoint (Must be at the end of the file) ---
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=port,
        path="/mcp",
        log_level="info"
    )
