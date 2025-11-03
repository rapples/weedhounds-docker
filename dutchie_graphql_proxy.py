#!/usr/bin/env python3
"""
Dutchie GraphQL Proxy Service
Uses the working browser context from the token service to make GraphQL queries
"""

import asyncio
import json
import logging
import os
import requests
import urllib.parse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dutchie GraphQL Proxy",
    description="Proxy service for Dutchie GraphQL queries using working browser context",
    version="1.0.0"
)

class GraphQLRequest(BaseModel):
    operationName: Optional[str] = None
    variables: Optional[Dict[str, Any]] = None
    query: Optional[str] = None
    extensions: Optional[Dict[str, Any]] = None

def get_session_data():
    """Get session data from token service"""
    try:
        # Always use host.docker.internal when running in Docker container
        # Check if we're in Docker by looking for /.dockerenv file
        in_docker = os.path.exists('/.dockerenv')
        token_service_url = 'http://host.docker.internal:8888/session' if in_docker else 'http://127.0.0.1:8888/session'
        logger.info(f"Connecting to token service at: {token_service_url}")
        response = requests.get(token_service_url, timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Token service error: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Cannot connect to token service: {e}")
        return None

@app.get("/")
async def root():
    """Service status"""
    return {
        "service": "Dutchie GraphQL Proxy",
        "status": "running",
        "description": "Proxy for Dutchie GraphQL using working browser context"
    }

@app.get("/health")
async def health():
    """Health check that verifies token service connectivity"""
    session_data = get_session_data()
    if session_data and session_data.get('token'):
        return {
            "status": "healthy",
            "token_service": "connected",
            "token_available": True,
            "cookies": len(session_data.get('cookies', {}))
        }
    else:
        return {
            "status": "unhealthy", 
            "token_service": "disconnected",
            "token_available": False,
            "error": "Cannot get session data"
        }

class SimplifiedProductsRequest(BaseModel):
    dutchie_id: str
    size: str = "3.5"
    thc_min: float = 25.0
    thc_max: float = 50.0
    pricing_type: str = "med"  # "med" or "rec"

@app.post("/products/flower")
async def get_flower_products(request: SimplifiedProductsRequest):
    """
    Simplified endpoint to get flower products using the correct Dutchie FilteredProducts API
    """
    try:
        session_data = get_session_data()
        if not session_data:
            raise HTTPException(status_code=503, detail="Token service unavailable")
        
        token = session_data.get('token')
        cookies = session_data.get('cookies', {})
        
        if not token:
            raise HTTPException(status_code=503, detail="No valid token available")
        
        logger.info(f"🎯 Simplified query: dutchie_id={request.dutchie_id}, size={request.size}, THC={request.thc_min}-{request.thc_max}%")
        
        # Convert size to option format (e.g., "3.5" -> "1/8oz")
        size_mapping = {
            "3.5": "1/8oz",
            "7": "1/4oz", 
            "14": "1/2oz",
            "28": "1oz"
        }
        option = size_mapping.get(request.size, "1/8oz")
        
        # Build the correct FilteredProducts query parameters
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "productIds": [],
                "dispensaryId": request.dutchie_id,
                "option": option,
                "pricingType": request.pricing_type,
                "strainTypes": [],
                "subcategories": [],
                "Status": "Active",
                "THCContent": {
                    "range": [request.thc_min, request.thc_max],
                    "unit": "PERCENTAGE"
                },
                "types": ["Flower"],
                "useCache": False,
                "isDefaultSort": False,
                "sortBy": "price",
                "sortDirection": 1,
                "bypassOnlineThresholds": False,
                "isKioskMenu": False,
                "removeProductsBelowOptionThresholds": True
            },
            "page": 0,
            "perPage": 100
        }
        
        # Use persisted query with the correct hash from your curl command
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
            }
        }
        
        # Prepare query data for GET request (like the working curl)
        import urllib.parse
        query_params = {
            "operationName": "FilteredProducts",
            "variables": json.dumps(variables),
            "extensions": json.dumps(extensions)
        }
        
        # Build URL with query parameters
        base_url = "https://dutchie.com/api-2/graphql"
        query_string = urllib.parse.urlencode(query_params)
        full_url = f"{base_url}?{query_string}"
        
        # Headers based on your working curl command
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'apollographql-client-name': 'Marketplace (production)',
            'Content-Type': 'application/json',
            'DNT': '1',
            'Priority': 'u=1, i',
            'Referer': f'https://dutchie.com/embedded-menu/ayr-wellness-bryn-mawr/products/flower?potencythc={request.thc_min}%2C{request.thc_max}&sortby=pricelowtohigh&weight=1-8oz',
            'Sec-CH-UA': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Storage-Access': 'active',
            'URL': f'https://dutchie.com/embedded-menu/ayr-wellness-bryn-mawr/products/flower?potencythc={request.thc_min}%2C{request.thc_max}&sortby=pricelowtohigh&weight=1-8oz',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'X-Dutchie-Session': token
        }
        
        # Make GET request with session cookies
        session = requests.Session()
        session.cookies.update(cookies)
        
        response = session.get(full_url, headers=headers, timeout=30)
        
        logger.info(f"📊 FilteredProducts response: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Success! Response structure: {type(result)} with keys: {list(result.keys()) if isinstance(result, dict) else 'not dict'}")
            
            # Extract products from the correct response structure
            if 'data' in result and 'filteredProducts' in result['data']:
                products = result['data']['filteredProducts']['products']
                total_count = result['data']['filteredProducts']['queryInfo']['totalCount']
                logger.info(f"🌸 Found {len(products)} products (total: {total_count})")
                
                # Convert to simplified format for easier consumption
                simplified_products = []
                for product in products:
                    simplified_product = {
                        'id': product.get('id'),
                        'name': product.get('Name'),
                        'brand': product.get('brandName'),
                        'type': product.get('type'),
                        'strain_type': product.get('strainType'),
                        'thc_content': product.get('THCContent', {}).get('range', [0])[0] if product.get('THCContent') else 0,
                        'cbd_content': product.get('CBDContent', {}).get('range', [0])[0] if product.get('CBDContent') else 0,
                        'prices': product.get('Prices', []),
                        'options': product.get('Options', []),
                        'image': product.get('Image'),
                        'effects': product.get('effects', {}),
                        'cannabinoids': product.get('cannabinoidsV2', [])
                    }
                    simplified_products.append(simplified_product)
                
                return {
                    "success": True,
                    "products": simplified_products,
                    "total_count": total_count,
                    "query_params": {
                        "dutchie_id": request.dutchie_id,
                        "size": request.size,
                        "thc_range": f"{request.thc_min}-{request.thc_max}%",
                        "pricing_type": request.pricing_type
                    }
                }
            else:
                logger.error(f"❌ Unexpected response structure: {result}")
                raise HTTPException(status_code=500, detail="Unexpected response structure")
        else:
            logger.error(f"❌ HTTP {response.status_code}: {response.text[:200]}")
            raise HTTPException(status_code=response.status_code, detail=f"Dutchie API error: {response.status_code}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Simplified query error: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.post("/graphql")
async def proxy_graphql(request: GraphQLRequest):
    """
    Proxy GraphQL requests to Dutchie using the working browser context
    """
    try:
        # Get fresh session data
        session_data = get_session_data()
        if not session_data:
            raise HTTPException(status_code=503, detail="Token service unavailable")
        
        token = session_data.get('token')
        cookies = session_data.get('cookies', {})
        
        if not token:
            raise HTTPException(status_code=503, detail="No valid token available")
        
        logger.info(f"🎯 Proxying GraphQL query: {request.operationName}")
        
        # Prepare headers like a real browser
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',  # Remove br to avoid compression issues
            'Accept-Language': 'en-US,en;q=0.9',
            'apollographql-client-name': 'Marketplace (production)',
            'Content-Type': 'application/json',
            'Origin': 'https://dutchie.com',
            'Referer': 'https://dutchie.com/embedded-menu/ayr-wellness-bryn-mawr/products/flower',
            'Sec-CH-UA': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors', 
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Dutchie-Session': token
        }
        
        # Convert request to dict
        request_data = {
            "operationName": request.operationName,
            "variables": request.variables or {},
            "extensions": request.extensions or {}
        }
        
        if request.query:
            request_data["query"] = request.query
        
        # Make the request using requests with the session cookies
        session = requests.Session()
        session.cookies.update(cookies)
        
        response = session.post(
            "https://dutchie.com/api-2/graphql",
            json=request_data,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"📊 GraphQL response: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                if 'errors' in data:
                    logger.warning(f"⚠️ GraphQL errors: {data['errors']}")
                return data
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON decode error. Response text: {response.text[:200]}")
                logger.error(f"❌ Response headers: {dict(response.headers)}")
                # Try to decode if it's compressed
                if 'content-encoding' in response.headers:
                    encoding = response.headers['content-encoding']
                    logger.error(f"❌ Content is {encoding} encoded")
                    if encoding == 'br':
                        try:
                            import brotli
                            decompressed = brotli.decompress(response.content).decode('utf-8')
                            logger.info(f"✅ Decompressed response: {decompressed[:200]}")
                            return json.loads(decompressed)
                        except Exception as decomp_error:
                            logger.error(f"❌ Brotli decompression failed: {decomp_error}")
                    elif encoding == 'gzip':
                        try:
                            import gzip
                            decompressed = gzip.decompress(response.content).decode('utf-8')
                            logger.info(f"✅ Decompressed response: {decompressed[:200]}")
                            return json.loads(decompressed)
                        except Exception as decomp_error:
                            logger.error(f"❌ Gzip decompression failed: {decomp_error}")
                raise HTTPException(status_code=500, detail=f"Invalid JSON response: {str(e)}")
        elif response.status_code == 403:
            logger.error("❌ 403 Forbidden - Cloudflare blocking")
            raise HTTPException(status_code=403, detail="Request blocked by Cloudflare")
        else:
            logger.error(f"❌ HTTP {response.status_code}: {response.text[:200]}")
            raise HTTPException(status_code=response.status_code, detail=f"Dutchie API error: {response.status_code}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Proxy error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal proxy error: {str(e)}")

class QueryRequest(BaseModel):
    dutchie_id: str
    size: float
    thc_min: float
    thc_max: float

@app.post("/query")
async def simplified_query(request: QueryRequest):
    """
    Simplified query endpoint for Dutchie dispensary products
    """
    try:
        logger.info(f"🎯 Simplified query: dutchie_id={request.dutchie_id}, size={request.size}, THC={request.thc_min}-{request.thc_max}%")
        
        # Map size to option format
        size_mapping = {
            3.5: "1/8oz",
            7: "1/4oz", 
            14: "1/2oz",
            28: "1oz"
        }
        
        option = size_mapping.get(request.size, "1/8oz")
        
        # Build the GraphQL request that Dutchie expects (using the correct persisted query format)
        graphql_request = GraphQLRequest(
            operationName="FilteredProducts",
            variables={
                "productsFilter": {
                    "dispensaryId": request.dutchie_id,
                    "option": option,
                    "pricingType": "med",
                    "strainTypes": [],
                    "subcategories": [],
                    "Status": "Active",
                    "THCContent": {
                        "range": [request.thc_min, request.thc_max],
                        "unit": "PERCENTAGE"
                    },
                    "types": ["Flower"],
                    "useCache": False,
                    "isDefaultSort": False,
                    "sortBy": "price",
                    "sortDirection": 1,
                    "bypassOnlineThresholds": False,
                    "isKioskMenu": False,
                    "removeProductsBelowOptionThresholds": True
                },
                "page": 0,
                "perPage": 50
            },
            extensions={
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
                }
            }
        )
        
        # Use the existing GraphQL proxy functionality
        return await proxy_graphql(graphql_request)
        
    except Exception as e:
        logger.error(f"❌ Simplified query error: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/test/flower")
async def test_flower_query():
    """Test endpoint for flower products"""
    test_request = GraphQLRequest(
        operationName="FilteredProducts",
        variables={
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId": "613a9411f117b000bee2ce2a",
                "types": ["Flower"],
                "Status": "Active"
            },
            "page": 0,
            "perPage": 5
        },
        extensions={
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
            }
        }
    )
    
    return await proxy_graphql(test_request)

if __name__ == "__main__":
    logger.info("🚀 Starting Dutchie GraphQL Proxy Service...")
    uvicorn.run(app, host="0.0.0.0", port=8891, log_level="info")