"""
🐧 UBUNTU BROWSER PROXY - MEMORY OPTIMIZED
==========================================
Lightweight version of the browser proxy for Ubuntu systems with limited resources
"""

import asyncio
import logging
import time
from fastapi import FastAPI, HTTPException
from playwright.async_api import async_playwright
import uvicorn
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UbuntuDutchieBrowserProxy:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.session_ready = False
        self.current_cookies = {}
        self.last_request_time = time.time()
        self.reconnection_attempts = 0
        self.max_reconnection_attempts = 3

    async def start_browser(self):
        """Start browser with Ubuntu-optimized settings"""
        try:
            self.playwright = await async_playwright().start()
            
            # Ubuntu-optimized browser args for lower memory usage
            browser_args = [
                '--no-sandbox',                    # Required for Ubuntu/Docker
                '--disable-dev-shm-usage',         # Reduces memory usage
                '--disable-gpu',                   # Saves memory on headless
                '--disable-web-security',          # May help with CORS
                '--disable-features=TranslateUI',  # Saves memory
                '--disable-ipc-flooding-protection',
                '--memory-pressure-off',           # Disable memory pressure signals
                '--max_old_space_size=512',        # Limit V8 heap to 512MB
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows'
            ]
            
            self.browser = await self.playwright.chromium.launch(
                headless=True,  # Always headless on Ubuntu for memory savings
                args=browser_args
            )
            
            # Create context with minimal memory footprint
            self.context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},  # Smaller viewport
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Create page
            self.page = await self.context.new_page()
            
            # Navigate to Ayr Dispensaries (same as Windows version)
            logger.info("🌐 Navigating to Ayr Dispensaries...")
            await self.page.goto('https://ayr.com/dispensaries/', wait_until='networkidle')
            
            # Wait for user to complete state selection (manual process)
            logger.info("⏳ Waiting for state selection and age verification...")
            await self._wait_for_session_ready()
            
        except Exception as e:
            logger.error(f"❌ Failed to start browser: {e}")
            raise

    async def _wait_for_session_ready(self):
        """Wait for session to be ready with timeout"""
        max_wait_time = 300  # 5 minutes max
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                # Check if we have the expected cookies indicating successful setup
                cookies = await self.context.cookies()
                self.current_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
                
                # Look for Dutchie-specific cookies that indicate successful state selection
                if any(cookie for cookie in cookies if 'dutchie' in cookie['name'].lower()):
                    logger.info("🎉 Session ready - Dutchie cookies detected!")
                    self.session_ready = True
                    return
                    
                # Also check for general session indicators
                if len(cookies) > 5:  # Arbitrary threshold indicating activity
                    logger.info(f"📊 Found {len(cookies)} cookies, checking session status...")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error checking session status: {e}")
                
            await asyncio.sleep(10)  # Check every 10 seconds
        
        # Timeout reached
        logger.warning("⏰ Session setup timeout - continuing with limited functionality")
        self.session_ready = True  # Allow service to continue

    async def get_health_status(self):
        """Get proxy health status"""
        try:
            if not self.context:
                return {
                    "status": "unhealthy",
                    "session_active": False,
                    "context_valid": False,
                    "last_request": self.last_request_time,
                    "cookies_count": 0,
                    "reconnection_attempts": self.reconnection_attempts
                }
            
            # Test context validity
            cookies = await self.context.cookies()
            cookie_count = len(cookies)
            
            return {
                "status": "healthy",
                "session_active": self.session_ready,
                "context_valid": True,
                "last_request": self.last_request_time,
                "cookies_count": cookie_count,
                "reconnection_attempts": self.reconnection_attempts
            }
            
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return {
                "status": "degraded",
                "session_active": False,
                "context_valid": False,
                "error": str(e),
                "last_request": self.last_request_time,
                "reconnection_attempts": self.reconnection_attempts
            }

    async def make_request(self, url, method="GET", **kwargs):
        """Make HTTP request through the browser context"""
        try:
            if not self.session_ready or not self.context:
                raise HTTPException(status_code=503, detail="Browser session not ready")
            
            self.last_request_time = time.time()
            
            if method.upper() == "GET":
                response = await self.page.goto(url, wait_until='networkidle')
                content = await self.page.content()
                
                return {
                    "status": response.status,
                    "content": content,
                    "url": response.url
                }
            else:
                raise HTTPException(status_code=405, detail=f"Method {method} not supported")
                
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# FastAPI app
app = FastAPI(
    title="Ubuntu Dutchie Browser Proxy",
    description="Memory-optimized browser proxy for Ubuntu systems",
    version="1.0.0"
)

# Global proxy instance
proxy = UbuntuDutchieBrowserProxy()

@app.on_event("startup")
async def startup_event():
    """Initialize browser on startup"""
    logger.info("🚀 Starting Ubuntu Dutchie Browser Proxy...")
    try:
        await proxy.start_browser()
        logger.info("✅ Browser proxy ready")
    except Exception as e:
        logger.error(f"❌ Failed to start browser proxy: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down browser proxy...")
    await proxy.cleanup()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return await proxy.get_health_status()

@app.get("/proxy/{path:path}")
async def proxy_request(path: str):
    """Proxy requests through browser"""
    full_url = f"https://dutchie.com/{path}"
    return await proxy.make_request(full_url)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Ubuntu Dutchie Browser Proxy",
        "status": "running",
        "endpoints": ["/health", "/proxy/{path}"],
        "optimized_for": "Ubuntu systems with limited resources"
    }

if __name__ == "__main__":
    # Ubuntu-optimized uvicorn settings
    uvicorn.run(
        app, 
        host="0.0.0.0",  # LAN accessible
        port=8889,
        workers=1,       # Single worker to save memory
        log_level="info",
        access_log=False  # Disable access logs to save memory
    )