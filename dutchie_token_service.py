#!/usr/bin/env python3
"""
Dutchie Token Service
A background service that continuously maintains fresh Dutchie session tokens
and serves them instantly to API clients via REST endpoints.

This eliminates the 15-second Playwright startup delay by running token
capture continuously in the background.
"""

import asyncio
import glob
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dutchie Token Service",
    description="Background service for fresh Dutchie session tokens",
    version="1.0.0"
)

class DutchieTokenService:
    """
    Background service that maintains fresh Dutchie session tokens
    """
    
    def __init__(self):
        self.current_token: Optional[str] = None
        self.current_cookies: Optional[Dict] = None
        self.token_expiry: Optional[datetime] = None
        self.last_capture_time: Optional[datetime] = None
        self.capture_in_progress: bool = False
        self.service_start_time = datetime.now()
        self.total_captures = 0
        self.successful_captures = 0
        self.playwright_instance = None
        self.browser = None
        
        # Request tracking for idle/active management
        self.last_request_time: Optional[datetime] = None
        self.total_requests = 0
        self.is_idle = False
        self.idle_threshold = 300  # 5 minutes = 300 seconds
        
        # Configuration
        self.refresh_interval = 30   # Refresh every 30 seconds when active
        self.idle_check_interval = 60  # Check for activity every 60 seconds when idle
        self.token_lifetime = 60     # Assume 1-minute token lifetime (more realistic)
        self.store_url = "https://ayrdispensaries.com/pennsylvania/bryn-mawr/shop/"
        self.dispensary_url = "https://ayrdispensaries.com/pennsylvania/bryn-mawr/shop/?dtche%5Bsortby%5D=pricelowtohigh&dtche%5Bpotencythc%5D=29%2C50&dtche%5Bcategory%5D=flower"
        self.store_id = "613a9411f117b000bee2ce2a"
        
        # Interactive mouse position config
        self.age_verification_config_file = "age_verification_positions.json"
        self.load_age_verification_config()
    
    def load_age_verification_config(self):
        """Load saved mouse positions for age verification"""
        try:
            if os.path.exists(self.age_verification_config_file):
                with open(self.age_verification_config_file, 'r') as f:
                    self.age_verification_positions = json.load(f)
                logger.info(f"📍 Loaded {len(self.age_verification_positions)} saved age verification positions")
            else:
                self.age_verification_positions = {}
                logger.info("📍 No saved age verification positions found")
        except Exception as e:
            logger.warning(f"⚠️ Could not load age verification config: {e}")
            self.age_verification_positions = {}
    
    def save_age_verification_config(self):
        """Save mouse positions for age verification"""
        try:
            with open(self.age_verification_config_file, 'w') as f:
                json.dump(self.age_verification_positions, f, indent=2)
            logger.info(f"💾 Saved age verification positions to {self.age_verification_config_file}")
        except Exception as e:
            logger.error(f"❌ Could not save age verification config: {e}")
    
    def track_api_request(self):
        """Track an API request to manage idle/active state"""
        self.last_request_time = datetime.now()
        self.total_requests += 1
        
        # If we were idle and got a request, wake up immediately
        if self.is_idle:
            logger.info("🌟 Service waking up from idle state - API request detected")
            self.is_idle = False
            # Trigger immediate token refresh if token is stale
            if self.should_refresh_token():
                asyncio.create_task(self.capture_fresh_token())
    
    def check_idle_status(self) -> bool:
        """Check if service should go idle (no requests for 5 minutes)"""
        if not self.last_request_time:
            return False  # Never had a request, don't go idle yet
            
        time_since_last_request = (datetime.now() - self.last_request_time).total_seconds()
        should_be_idle = time_since_last_request > self.idle_threshold
        
        if should_be_idle and not self.is_idle:
            logger.info(f"😴 Service going idle - no requests for {time_since_last_request:.0f}s")
            self.is_idle = True
        elif not should_be_idle and self.is_idle:
            logger.info("🌟 Service becoming active")
            self.is_idle = False
            
        return self.is_idle
    
    def should_refresh_token(self) -> bool:
        """Check if token needs refreshing"""
        return (
            not self.current_token or
            not self.token_expiry or
            datetime.now() >= self.token_expiry - timedelta(seconds=60)  # Refresh 1 minute before expiry
        )
    
    async def handle_interactive_age_verification(self, page):
        """Interactive age verification with mouse position recording"""
        try:
            # Check if we have a saved position for this domain
            url = page.url
            domain = url.split('/')[2] if '//' in url else 'unknown'
            
            # Look for age verification indicators on the page
            age_indicators = [
                'age', 'verification', '21', 'over', 'confirm', 'enter site', 
                'continue', 'yes', 'accept', 'proceed'
            ]
            
            page_content = await page.content()
            page_text = page_content.lower()
            
            has_age_verification = any(indicator in page_text for indicator in age_indicators)
            
            if has_age_verification:
                logger.info("🔞 Age verification detected on page!")
                
                # Check if we have a saved position for this domain
                if domain in self.age_verification_positions:
                    position = self.age_verification_positions[domain]
                    logger.info(f"📍 Using saved position for {domain}: ({position['x']}, {position['y']})")
                    
                    try:
                        # Enhanced clicking approach with window focus
                        logger.info("🔍 Focusing browser window and preparing click...")
                        
                        # Step 1: Bring browser window to foreground and focus
                        await page.bring_to_front()
                        await asyncio.sleep(0.5)
                        
                        # Step 2: Focus on the page
                        await page.focus('body')
                        await asyncio.sleep(0.5)
                        
                        # Step 3: Move mouse to position to ensure browser knows where we are
                        await page.mouse.move(position['x'], position['y'])
                        await asyncio.sleep(0.5)
                        
                        # Step 4: Try clicking with element targeting (more reliable)
                        try:
                            # Try to find clickable element at position
                            element_at_point = await page.evaluate(f'''
                                () => {{
                                    const element = document.elementFromPoint({position['x']}, {position['y']});
                                    return element ? element.tagName + ' ' + (element.className || '') : null;
                                }}
                            ''')
                            if element_at_point:
                                logger.info(f"🎯 Found element at position: {element_at_point}")
                            
                            # Perform the click using page coordinates
                            await page.mouse.click(position['x'], position['y'])
                            logger.info("🖱️ First click performed")
                            await asyncio.sleep(1)
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Element targeting failed: {e}")
                        
                        # Step 5: If first click didn't work, try more aggressive clicking
                        # Check if we need to try again
                        current_content = await page.content()
                        if len(current_content) == len(page_content):
                            logger.info("🔄 First click didn't work, trying more aggressive approach...")
                            
                            # Method 2: Double click with force
                            await page.mouse.click(position['x'], position['y'], click_count=2)
                            logger.info("🖱️ Double click performed")
                            await asyncio.sleep(1)
                            
                            # Method 3: Try with explicit button down/up
                            await page.mouse.move(position['x'], position['y'])
                            await page.mouse.down()
                            await asyncio.sleep(0.2)
                            await page.mouse.up()
                            logger.info("🖱️ Button down/up performed")
                            await asyncio.sleep(1)
                            
                            # Method 4: Last resort - system-level click using pyautogui
                            check_content = await page.content()
                            if len(check_content) == len(page_content):
                                logger.info("🔄 Browser clicks failed, trying system-level click...")
                                try:
                                    import pyautogui
                                    # Focus browser window first
                                    pyautogui.click(position['x'], position['y'])
                                    logger.info("🖱️ System-level click performed")
                                    await asyncio.sleep(2)
                                except Exception as sys_e:
                                    logger.warning(f"⚠️ System-level click failed: {sys_e}")
                        
                        # Step 6: Final check
                        await asyncio.sleep(2)
                        final_content = await page.content()
                        if len(final_content) != len(page_content):
                            logger.info("✅ Age verification handled with saved position!")
                            return True
                        else:
                            logger.warning("⚠️ Saved position didn't work, falling back to interactive mode")
                    except Exception as e:
                        logger.warning(f"⚠️ Saved position failed: {e}")
                
                # Interactive mode: Ask user to position mouse
                logger.info("🖱️  INTERACTIVE AGE VERIFICATION MODE")
                logger.info("📋 INSTRUCTIONS:")
                logger.info("   1. A browser window should be open with the age verification page")
                logger.info("   2. Move your mouse to the age verification button (usually blue, says 'Yes' or 'Continue')")
                logger.info("   3. Press ENTER on this terminal when your mouse is positioned correctly")
                logger.info("   4. The system will record that position for future use")
                logger.info("")
                logger.info("🎯 Ready! Move your mouse to the button and press ENTER...")
                
                # Wait for user input
                import sys
                try:
                    user_input = input("Press ENTER when mouse is positioned over the age verification button: ")
                    
                    # Get current mouse position
                    import pyautogui
                    mouse_x, mouse_y = pyautogui.position()
                    
                    logger.info(f"📍 Recorded position: ({mouse_x}, {mouse_y})")
                    
                    # Save this position
                    self.age_verification_positions[domain] = {
                        'x': mouse_x,
                        'y': mouse_y,
                        'recorded_at': datetime.now().isoformat(),
                        'url': url
                    }
                    self.save_age_verification_config()
                    
                    # Click the recorded position
                    await page.mouse.click(mouse_x, mouse_y)
                    await asyncio.sleep(3)
                    
                    logger.info("✅ Age verification click completed!")
                    return True
                    
                except KeyboardInterrupt:
                    logger.info("⏭️ Skipping interactive age verification")
                    return False
                except Exception as e:
                    logger.error(f"❌ Interactive age verification failed: {e}")
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Interactive age verification failed: {e}")
            return False
    
    def cleanup_old_token_files(self):
        """Clean up old token files that are 2+ minutes past their expiration time"""
        try:
            # Find all token files in current directory
            token_files = glob.glob("dutchie_tokens_*.json")
            
            if not token_files:
                return
            
            current_time = datetime.now()
            cleaned_count = 0
            
            for file_path in token_files:
                try:
                    # Read the token file to get expiration info
                    with open(file_path, 'r') as f:
                        token_data = json.load(f)
                    
                    # Get expiration time from the file
                    expires_at_str = token_data.get('expires_at')
                    if not expires_at_str:
                        # If no expiration info, check file age as fallback
                        file_stat = os.stat(file_path)
                        file_age = current_time - datetime.fromtimestamp(file_stat.st_mtime)
                        if file_age > timedelta(minutes=5):  # Delete files older than 5 minutes if no expiry
                            os.remove(file_path)
                            cleaned_count += 1
                            logger.info(f"🗑️ Cleaned old token file (no expiry): {file_path}")
                        continue
                    
                    # Parse expiration time
                    expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                    if expires_at.tzinfo is not None:
                        # Convert to local time if timezone aware
                        expires_at = expires_at.replace(tzinfo=None)
                    
                    # Check if file is 2+ minutes past expiration
                    time_since_expiry = current_time - expires_at
                    if time_since_expiry > timedelta(minutes=2):
                        os.remove(file_path)
                        cleaned_count += 1
                        logger.info(f"🗑️ Cleaned expired token file: {file_path} (expired {time_since_expiry.total_seconds():.0f}s ago)")
                
                except Exception as e:
                    logger.warning(f"⚠️ Could not process token file {file_path}: {e}")
                    # If we can't read the file properly, and it's older than 10 minutes, delete it
                    try:
                        file_stat = os.stat(file_path)
                        file_age = current_time - datetime.fromtimestamp(file_stat.st_mtime)
                        if file_age > timedelta(minutes=10):
                            os.remove(file_path)
                            cleaned_count += 1
                            logger.info(f"🗑️ Cleaned unreadable old token file: {file_path}")
                    except:
                        pass
            
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned up {cleaned_count} old token files")
        
        except Exception as e:
            logger.warning(f"⚠️ Token file cleanup error: {e}")
        
    async def initialize(self):
        """Initialize the Playwright browser for token capture with enhanced settings"""
        try:
            logger.info("🚀 Initializing Dutchie Token Service...")
            
            # Clean up any old token files first
            self.cleanup_old_token_files()
            
            self.playwright_instance = await async_playwright().start()
            
            # Check if running in Docker container and start Xvfb if needed
            is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER', False)
            
            if is_docker:
                # Start Xvfb for virtual display in Docker
                import subprocess
                import time
                
                logger.info("🖥️ Starting Xvfb virtual display for Docker...")
                subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'], 
                               stdout=subprocess.DEVNULL, 
                               stderr=subprocess.DEVNULL)
                time.sleep(2)  # Give Xvfb time to start
                os.environ['DISPLAY'] = ':99'
                logger.info("✅ Xvfb virtual display started on :99")
            
            # Launch browser with enhanced settings for better success rate
            self.browser = await self.playwright_instance.chromium.launch(
                headless=False,  # Can now use headed mode with Xvfb
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                ]
            )
            logger.info(f"✅ Playwright browser initialized with enhanced settings (Docker: {is_docker})")
            
            # Capture initial token with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                logger.info(f"🎯 Initial token capture attempt {attempt + 1}/{max_retries}")
                success = await self.capture_fresh_token()
                if success:
                    logger.info("🎉 Initial token capture successful!")
                    break
                elif attempt < max_retries - 1:
                    logger.info(f"⏳ Waiting 10 seconds before retry...")
                    await asyncio.sleep(10)
                else:
                    logger.warning("⚠️ All initial capture attempts failed, continuing with service...")
            
            # Start background refresh loop
            asyncio.create_task(self.token_refresh_loop())
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize service: {e}")
            raise
            
    async def shutdown(self):
        """Cleanup resources"""
        logger.info("🛑 Shutting down Dutchie Token Service...")
        if self.browser:
            await self.browser.close()
        if self.playwright_instance:
            await self.playwright_instance.stop()
        logger.info("✅ Service shutdown complete")
        
    async def capture_fresh_token(self) -> bool:
        """
        Capture a fresh Dutchie session token using Playwright with enhanced detection
        Uses the proven working approach that captured 38+ successful API calls
        Returns True if successful, False otherwise
        """
        if self.capture_in_progress:
            logger.info("⏳ Token capture already in progress, skipping...")
            return False
            
        self.capture_in_progress = True
        self.total_captures += 1
        
        try:
            logger.info(f"🔄 Capturing fresh Dutchie token (attempt #{self.total_captures})")
            
            # Create new context with realistic browser settings
            context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
            )
            
            # Create new page for token capture
            page = await context.new_page()
            
            # Enhanced request interception with detailed logging
            captured_tokens = []
            captured_cookies = {}
            
            async def handle_request(request):
                url = request.url
                headers = request.headers
                
                # Look for Dutchie GraphQL endpoints
                if ('dutchie.com/api-2/graphql' in url or 'dutchie.com/graphql' in url):
                    logger.info(f"🎯 INTERCEPTED DUTCHIE API: {request.method} {url[:100]}...")
                    
                    # Extract authorization token from multiple possible header formats
                    auth_header = None
                    token = None
                    
                    # Check for x-dutchie-session header (this is the main one!)
                    if 'x-dutchie-session' in headers:
                        token = headers['x-dutchie-session']
                        captured_tokens.append(token)
                        logger.info(f"🔑 Session Token (x-dutchie-session): {token[:20]}...")
                    
                    # Check for standard authorization header
                    elif 'authorization' in headers:
                        auth_header = headers['authorization']
                        if auth_header.startswith('Bearer '):
                            token = auth_header.replace('Bearer ', '')
                            captured_tokens.append(token)
                            logger.info(f"🔑 Session Token (Bearer): {token[:20]}...")
                    
                    # Check for other possible token headers
                    else:
                        token_headers = ['x-auth-token', 'x-session-token', 'x-token', 'token', 'session']
                        for header_name in token_headers:
                            if header_name in headers:
                                token = headers[header_name]
                                captured_tokens.append(token)
                                logger.info(f"🔑 Session Token ({header_name}): {token[:20]}...")
                                break
                    
                    # Check cookies for session tokens
                    if 'cookie' in headers:
                        cookie_header = headers['cookie']
                        # Look for session-like cookies
                        if 'session' in cookie_header.lower() or 'token' in cookie_header.lower():
                            logger.info(f"🍪 Cookie header: {cookie_header[:100]}...")
                    
                    # Log all headers for debugging
                    if not token:
                        logger.info(f"📋 All headers: {list(headers.keys())}")
                        for key, value in headers.items():
                            if any(keyword in key.lower() for keyword in ['auth', 'token', 'session', 'bear']):
                                logger.info(f"🔍 {key}: {value[:50]}...")
                    
                    # Log important headers
                    important_headers = ['apollographql-client-name', 'apollographql-client-version', 'referer']
                    for header in important_headers:
                        if header in headers:
                            logger.info(f"📋 {header.title()}: {headers[header][:50]}...")
            
            async def handle_response(response):
                url = response.url
                if ('dutchie.com/api-2/graphql' in url or 'dutchie.com/graphql' in url):
                    logger.info(f"📡 RESPONSE: {response.status} for {url[:100]}...")
                    
                    # Try to extract products from response
                    try:
                        if response.status == 200:
                            text = await response.text()
                            if 'products' in text and len(text) > 1000:
                                # Count products in response
                                import re
                                product_matches = re.findall(r'"products":\s*\[', text)
                                if product_matches:
                                    # Estimate product count
                                    product_count = text.count('"id":') - 5  # Rough estimate
                                    if product_count > 0:
                                        logger.info(f"🌿 Found {product_count} products in response")
                    except Exception as e:
                        logger.debug(f"Could not parse response: {e}")
            
            page.on('request', handle_request)
            page.on('response', handle_response)
            
            # Set cookies from your Chrome session
            logger.info("🔞 Pre-setting cookies from Chrome session...")
            try:
                # Exact cookies from your Chrome session
                chrome_cookies = [
                    {'name': 'x-rocket-cookies', 'value': '0', 'domain': '.ayrdispensaries.com'},
                    {'name': '_cflb', 'value': '04dToaXWHAPESU8RyBQBc8Yta2dmM5dZAtsWYdfwNF', 'domain': '.dutchie.com'},
                    {'name': '__ssid', 'value': '97303a68ba9a47c33ac278146413df5', 'domain': '.dutchie.com'},
                    {'name': '_ga', 'value': 'GA1.2.457508604.1759875098', 'domain': '.dutchie.com'},
                    {'name': '_gid', 'value': 'GA1.2.1017391196.1759875098', 'domain': '.dutchie.com'},
                ]
                
                for cookie in chrome_cookies:
                    try:
                        await context.add_cookies([cookie])
                        logger.debug(f"🍪 Pre-set Chrome cookie: {cookie['name']}")
                    except Exception as e:
                        logger.debug(f"Failed to set cookie {cookie['name']}: {e}")
                        
            except Exception as e:
                logger.warning(f"⚠️ Failed to pre-set Chrome cookies: {e}")
            
            # Navigate directly to Dutchie embed URL 
            logger.info(f"🌐 Navigating to {self.store_url}")
            
            try:
                await page.goto(self.store_url, wait_until='domcontentloaded', timeout=30000)
                logger.info("✅ Dutchie page loaded successfully")
            except Exception as e:
                logger.warning(f"⚠️ Page load timeout/error (continuing): {e}")
            
            # Handle additional age verification modal on Dutchie if present
            logger.info("🔞 Checking for Dutchie age verification modal...")
            
            try:
                # First, try interactive mouse position recording for age verification
                age_verified = await self.handle_interactive_age_verification(page)
                
                if not age_verified:
                    # Fall back to automatic selector-based detection
                    # Wait a moment for modal to appear
                    await asyncio.sleep(2)
                
                # Look for common age verification patterns with more specific targeting
                age_selectors = [
                    'button:has-text("Yes, I am 21")',
                    'button:has-text("I am 21")', 
                    'button:has-text("Yes, I am 21 or older")',
                    'button:has-text("21+")',
                    'button:has-text("Continue")',
                    'button:has-text("Enter Site")',
                    'button:has-text("Yes")',
                    'button:has-text("I\'m 21+")',
                    'button:has-text("I am 21+")',
                    'button:has-text("Confirm")',
                    'button:has-text("Proceed")',
                    'button:has-text("Accept")',
                    '[data-testid="age-verification-yes"]',
                    '[data-testid="age-verification-continue"]',
                    '[data-testid*="age"][data-testid*="yes"]',
                    '[data-testid*="age"][data-testid*="continue"]',
                    '[data-testid*="verify"]',
                    '[data-testid*="21"]',
                    'button[aria-label*="age"]',
                    'button[aria-label*="21"]',
                    '.age-verification button',
                    '.modal button:has-text("Yes")',
                    '.age-gate button',
                    '#age-verification button',
                    '[role="dialog"] button:has-text("Yes")',
                    '[role="dialog"] button:has-text("Continue")',
                    'button[style*="blue"]',
                    'button[class*="blue"]',
                    'button[class*="primary"]',
                    'button[class*="confirm"]',
                    'button[class*="accept"]',
                    '.center button',
                    '.centered button',
                    'div[style*="center"] button',
                    'div[style*="middle"] button',
                    'button[type="submit"]',
                    'button[aria-label*="21"]',
                    '.age-verification button',
                    '.modal button:has-text("Yes")',
                    '.age-gate button',
                    '#age-verification button',
                    '[role="dialog"] button:has-text("Yes")',
                    '[role="dialog"] button:has-text("Continue")'
                ]
                
                age_verified = False
                for selector in age_selectors:
                    try:
                        # Check if selector exists
                        element = await page.query_selector(selector)
                        if element and await element.is_visible():
                            # Get button text for verification
                            text = await element.text_content()
                            logger.info(f"🔞 Found age verification button: {selector} (text: '{text}')")
                            
                            # Click and wait for response
                            await element.click()
                            await asyncio.sleep(3)
                            
                            # Verify the modal disappeared
                            still_visible = await element.is_visible()
                            if not still_visible:
                                age_verified = True
                                logger.info("✅ Age verification completed successfully")
                                break
                            else:
                                logger.info(f"⚠️ Age verification button still visible after click")
                        
                    except Exception as e:
                        logger.debug(f"Age selector {selector} failed: {e}")
                        continue
                
                if not age_verified:
                    # Try more generic approaches - look for any buttons in modals/dialogs
                    try:
                        modal_buttons = await page.query_selector_all('.modal button, [role="dialog"] button, .dialog button, [class*="modal"] button, [class*="overlay"] button')
                        for button in modal_buttons:
                            if not await button.is_visible():
                                continue
                                
                            text = await button.text_content() or ""
                            text_lower = text.lower().strip()
                            
                            # Look for age-related keywords
                            if any(keyword in text_lower for keyword in ['yes', '21', 'continue', 'enter', 'confirm', 'proceed', 'accept']):
                                logger.info(f"🔞 Clicking potential age verification: '{text}' (generic approach)")
                                await button.click()
                                await asyncio.sleep(3)
                                
                                # Check if button disappeared
                                still_visible = await button.is_visible()
                                if not still_visible:
                                    age_verified = True
                                    logger.info("✅ Age verification completed via generic button")
                                    break
                    except Exception as e:
                        logger.debug(f"Generic age verification failed: {e}")
                
                if age_verified:
                    # Wait for page to reload/settle after age verification
                    logger.info("⏳ Waiting for page to reload after age verification...")
                    await asyncio.sleep(5)
                    
                    # Check for age verification cookies
                    cookies_after_age = await context.cookies()
                    age_cookies = [c for c in cookies_after_age if any(keyword in c.get('name', '').lower() for keyword in ['age', 'verify', 'adult', 'over21', 'consent'])]
                    if age_cookies:
                        logger.info(f"🍪 Captured {len(age_cookies)} age verification cookies: {[c['name'] for c in age_cookies]}")
                    else:
                        logger.warning("⚠️ No age verification cookies detected")
                        
                else:
                    logger.info("ℹ️ No age verification modal detected or handled")
                    
            except Exception as e:
                logger.warning(f"⚠️ Age verification handling error: {e}")

            # Wait for API calls to be made and interact with page
            logger.info("⏳ Waiting for API calls and simulating user interaction...")
            
            # Try to trigger more API calls by interacting with the page
            try:
                # Wait for page to settle
                await asyncio.sleep(3)
                
                # Look for filter buttons or product categories to click
                filter_selectors = [
                    'button[data-testid*="filter"]',
                    'button[data-testid*="category"]', 
                    'button:has-text("Flower")',
                    'button:has-text("Filter")',
                    '[role="button"]:has-text("Flower")',
                    '.filter-button',
                    '.category-button'
                ]
                
                for selector in filter_selectors:
                    try:
                        element = await page.query_selector(selector)
                        if element:
                            logger.info(f"🖱️ Clicking filter: {selector}")
                            await element.click()
                            await asyncio.sleep(2)  # Wait for API call
                            break
                    except:
                        continue
                
                # Try scrolling to trigger lazy loading
                logger.info("📜 Scrolling to trigger more API calls...")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
                await asyncio.sleep(2)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.warning(f"⚠️ Page interaction error (continuing): {e}")
            
            # Get cookies for enhanced token validation  
            try:
                cookies = await context.cookies()
                for cookie in cookies:
                    # Capture ALL cookies from dutchie.com domain (including age verification)
                    domain = cookie.get('domain', '').lower()
                    if 'dutchie.com' in domain or domain.endswith('.dutchie.com'):
                        captured_cookies[cookie['name']] = cookie['value']
                        logger.debug(f"🍪 Captured cookie: {cookie['name']} from {domain}")
                        
                if captured_cookies:
                    logger.info(f"🍪 Captured {len(captured_cookies)} cookies from dutchie.com")
                    # Log cookie names for debugging
                    cookie_names = list(captured_cookies.keys())
                    logger.info(f"🔍 Cookie names: {cookie_names}")
                    
                    # Check for age verification cookies specifically
                    age_cookies = [name for name in cookie_names if any(keyword in name.lower() for keyword in ['age', 'verify', 'adult', '21', 'confirm'])]
                    if age_cookies:
                        logger.info(f"🔞 Age verification cookies found: {age_cookies}")
                    else:
                        logger.warning("⚠️ No age verification cookies detected")
            except Exception as e:
                logger.warning(f"⚠️ Cookie capture error: {e}")
            
            # Close page and context
            await page.close()
            await context.close()
            
            # Process captured tokens
            if captured_tokens:
                # Use the most recent token
                latest_token = captured_tokens[-1]
                self.current_token = latest_token
                self.current_cookies = captured_cookies
                self.token_expiry = datetime.now() + timedelta(seconds=self.token_lifetime)
                self.last_capture_time = datetime.now()
                self.successful_captures += 1
                
                logger.info(f"✅ Token captured successfully! {len(captured_tokens)} tokens found")
                logger.info(f"🍪 Stored {len(captured_cookies)} cookies")
                logger.info(f"🕒 Token expires: {self.token_expiry}")
                
                # Save token with cookies to file for backup
                await self.save_enhanced_token_to_file(latest_token, captured_cookies)
                
                return True
            else:
                logger.warning(f"⚠️ No tokens captured. Captured {len(captured_cookies)} cookies.")
                return False
                
        except Exception as e:
            logger.error(f"❌ Token capture failed: {e}")
            return False
        finally:
            self.capture_in_progress = False
            
    async def save_enhanced_token_to_file(self, token: str, cookies: Dict):
        """Save captured token with cookies and metadata to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dutchie_tokens_{timestamp}.json"
            
            # Decode token to get expiry info
            token_info = {}
            try:
                import base64
                # Handle base64 padding
                padded_token = token
                padding = 4 - len(padded_token) % 4
                if padding != 4:
                    padded_token += '=' * padding
                
                decoded = base64.b64decode(padded_token)
                token_info = json.loads(decoded)
                logger.info(f"🔍 Decoded token info: {list(token_info.keys())}")
            except Exception as e:
                logger.warning(f"⚠️ Could not decode token: {e}")
            
            token_data = {
                "session_token": token,
                "token_info": token_info,
                "cookies": cookies,
                "captured_at": datetime.now().isoformat(),
                "expires_at": self.token_expiry.isoformat() if self.token_expiry else None,
                "store_id": self.store_id,
                "store_url": self.store_url,
                "capture_metadata": {
                    "total_captures": self.total_captures,
                    "successful_captures": self.successful_captures,
                    "service_uptime": int((datetime.now() - self.service_start_time).total_seconds())
                }
            }
            
            with open(filename, 'w') as f:
                json.dump(token_data, f, indent=2)
                
            logger.info(f"💾 Enhanced token saved to {filename}")
            logger.info(f"📊 Token info: {len(cookies)} cookies, {len(token_info)} token fields")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to save enhanced token to file: {e}")
            
    async def save_token_to_file(self, token: str):
        """Save captured token to JSON file for backup (legacy method)"""
        await self.save_enhanced_token_to_file(token, {})
            
    async def token_refresh_loop(self):
        """Background loop that refreshes tokens periodically with idle/active management"""
        logger.info(f"🔄 Starting intelligent token refresh loop")
        logger.info(f"📊 Active mode: {self.refresh_interval}s interval | Idle after: {self.idle_threshold}s")
        
        while True:
            try:
                # Check current idle status
                was_idle = self.is_idle
                is_currently_idle = self.check_idle_status()
                
                # Clean up old token files periodically
                self.cleanup_old_token_files()
                
                if is_currently_idle:
                    # In idle mode: only check for activity, don't refresh tokens
                    if not was_idle:
                        logger.info("💤 Entering idle mode - pausing token refresh to avoid detection")
                    
                    # Wait longer in idle mode and just check for activity
                    await asyncio.sleep(self.idle_check_interval)
                    continue
                else:
                    # Active mode: normal token refresh logic
                    if was_idle:
                        logger.info("⚡ Resuming active token refresh mode")
                    
                    # Check if we need a new token
                    needs_refresh = self.should_refresh_token()
                    
                    if needs_refresh:
                        logger.info("🔄 Token expired or expiring soon, capturing fresh token...")
                        await self.capture_fresh_token()
                    else:
                        if self.token_expiry:
                            time_until_expiry = (self.token_expiry - datetime.now()).total_seconds()
                            logger.info(f"✅ Token still valid for {time_until_expiry:.0f}s")
                        else:
                            logger.info("✅ Service active, monitoring for requests")
                    
                    # Wait standard interval in active mode
                    await asyncio.sleep(self.refresh_interval)
                    
            except Exception as e:
                logger.error(f"❌ Error in token refresh loop: {e}")
                await asyncio.sleep(30)  # Wait 30s before retrying
                
    def get_service_stats(self) -> Dict:
        """Get service statistics"""
        uptime = datetime.now() - self.service_start_time
        
        # Calculate idle time
        time_since_last_request = None
        if self.last_request_time:
            time_since_last_request = (datetime.now() - self.last_request_time).total_seconds()
        
        return {
            "service_status": "idle" if self.is_idle else "active",
            "uptime_seconds": int(uptime.total_seconds()),
            "current_token_available": self.current_token is not None,
            "token_expires_at": self.token_expiry.isoformat() if self.token_expiry else None,
            "last_capture_time": self.last_capture_time.isoformat() if self.last_capture_time else None,
            "last_request_time": self.last_request_time.isoformat() if self.last_request_time else None,
            "total_requests": self.total_requests,
            "time_since_last_request_seconds": time_since_last_request,
            "is_idle": self.is_idle,
            "idle_threshold_seconds": self.idle_threshold,
            "total_captures": self.total_captures,
            "successful_captures": self.successful_captures,
            "success_rate": f"{(self.successful_captures/self.total_captures*100):.1f}%" if self.total_captures > 0 else "0%",
            "capture_in_progress": self.capture_in_progress
        }

# Global service instance
token_service = DutchieTokenService()

@app.on_event("startup")
async def startup_event():
    """Initialize the service on startup"""
    await token_service.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await token_service.shutdown()

@app.get("/")
async def root():
    """Service health check"""
    return {
        "service": "Dutchie Token Service",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/token")
async def get_token():
    """Get the current fresh Dutchie session token"""
    # Track this API request for idle/active management
    token_service.track_api_request()
    
    if not token_service.current_token:
        raise HTTPException(status_code=503, detail="No token available")
        
    if token_service.token_expiry and datetime.now() >= token_service.token_expiry:
        raise HTTPException(status_code=503, detail="Token expired")
        
    return {
        "token": token_service.current_token,
        "expires_at": token_service.token_expiry.isoformat() if token_service.token_expiry else None,
        "captured_at": token_service.last_capture_time.isoformat() if token_service.last_capture_time else None
    }

@app.get("/session")
async def get_session():
    """Get the complete session data (token + cookies)"""
    # Track this API request for idle/active management
    token_service.track_api_request()
    
    if not token_service.current_token:
        raise HTTPException(status_code=503, detail="No session available")
        
    if token_service.token_expiry and datetime.now() >= token_service.token_expiry:
        raise HTTPException(status_code=503, detail="Session expired")
        
    return {
        "token": token_service.current_token,
        "cookies": token_service.current_cookies or {},
        "expires_at": token_service.token_expiry.isoformat() if token_service.token_expiry else None,
        "captured_at": token_service.last_capture_time.isoformat() if token_service.last_capture_time else None
    }

@app.get("/stats")
async def get_stats():
    """Get service statistics"""
    return token_service.get_service_stats()

@app.post("/refresh")
async def force_refresh():
    """Force a token refresh"""
    if token_service.capture_in_progress:
        return {"message": "Token capture already in progress", "status": "waiting"}
        
    success = await token_service.capture_fresh_token()
    
    if success:
        return {
            "message": "Token refreshed successfully",
            "token": token_service.current_token,
            "expires_at": token_service.token_expiry.isoformat() if token_service.token_expiry else None
        }
    else:
        raise HTTPException(status_code=500, detail="Token refresh failed")

@app.post("/cleanup")
async def force_cleanup():
    """Force cleanup of old token files"""
    try:
        # Count files before cleanup
        before_count = len(glob.glob("dutchie_tokens_*.json"))
        
        token_service.cleanup_old_token_files()
        
        # Count files after cleanup
        after_count = len(glob.glob("dutchie_tokens_*.json"))
        cleaned_count = before_count - after_count
        
        return {
            "message": "Token file cleanup completed",
            "files_before": before_count,
            "files_after": after_count,
            "files_cleaned": cleaned_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.post("/wakeup")
async def force_wakeup():
    """Force service to wake up from idle state"""
    was_idle = token_service.is_idle
    token_service.track_api_request()  # This will wake up the service
    
    # If we were idle and token is stale, capture a fresh one
    if was_idle and token_service.should_refresh_token():
        if not token_service.capture_in_progress:
            success = await token_service.capture_fresh_token()
            return {
                "message": "Service awakened and token refreshed",
                "was_idle": was_idle,
                "token_refreshed": success,
                "status": "active"
            }
    
    return {
        "message": "Service awakened" if was_idle else "Service was already active",
        "was_idle": was_idle,
        "status": "active"
    }

if __name__ == "__main__":
    logger.info("🚀 Starting Dutchie Token Service...")
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")