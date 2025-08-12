#!/usr/bin/env python3
"""
Test selenium-wire + SSL handling together (exactly like the main scraper uses).
This mimics the exact configuration used in linkedin_scraper.py
"""
import os
import sys
import time
import tempfile
from pathlib import Path
from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables
load_dotenv()

# Import from scraper config
try:
    from scraper.config import DI_HOST, DI_PORT, DI_USERNAME, DI_PASSWORD
    print(f"✅ Imported credentials from scraper.config")
except ImportError:
    DI_HOST = os.getenv("DI_HOST", "gw.dataimpulse.com")
    DI_PORT = os.getenv("DI_PORT", "823") 
    DI_USERNAME = os.getenv("DI_USERNAME")
    DI_PASSWORD = os.getenv("DI_PASSWORD")
    print(f"✅ Using environment variables")

def test_seleniumwire_ssl_combined():
    """Test selenium-wire with SSL handling exactly like linkedin_scraper.py"""
    print("🔍 TESTING SELENIUM-WIRE + SSL (EXACT MAIN SCRAPER CONFIG)")
    print("=" * 60)
    
    if not DI_USERNAME or not DI_PASSWORD:
        print("❌ ERROR: Missing credentials!")
        return False
    
    try:
        from seleniumwire import webdriver as wire_webdriver
        from selenium.webdriver.common.by import By
        
        # Set up selenium-wire certificate handling (EXACT COPY from linkedin_scraper.py)
        seleniumwire_cert_dir = tempfile.mkdtemp(prefix="seleniumwire_certs_")
        os.environ['SELENIUMWIRE_STORAGE_DIR'] = seleniumwire_cert_dir
        print(f"📁 Using certificate directory: {seleniumwire_cert_dir}")
        
        # Build auth username exactly like linkedin_scraper.py
        country = 'gb'
        auth_user = f"{DI_USERNAME}__cr.{country}"
        proxy_url = f"http://{auth_user}:{DI_PASSWORD}@{DI_HOST}:{DI_PORT}"
        
        print(f"🔑 Using credentials (EXACT COPY):")
        print(f"   Auth Username: {auth_user}")
        print(f"   Proxy: {DI_HOST}:{DI_PORT}")
        print(f"   Country: {country}")
        
        # Selenium-wire options - simplified for proxy but with SSL handling
        seleniumwire_options = {
            "proxy": {
                "http": proxy_url,
                "https": proxy_url,
            },
            # SSL options to prevent strikethrough HTTPS
            "verify_ssl": True,
            "suppress_connection_errors": False,
        }
        
        # Chrome options - minimal like working test but with SSL support
        options = wire_webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Basic stability arguments (like working test)
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=800,600')
        
        # Do NOT add aggressive SSL-disabling arguments
        # Let selenium-wire handle SSL properly
        
        print(f"🚀 Creating Chrome with EXACT main scraper config...")
        
        # Retry logic (EXACT COPY from linkedin_scraper.py)
        max_retries = 3
        retry_delay = 2
        driver = None
        
        for attempt in range(max_retries):
            try:
                # Add a small delay between attempts to avoid socket conflicts
                if attempt > 0:
                    time.sleep(retry_delay)
                    print(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                
                driver = wire_webdriver.Chrome(
                    options=options,
                    seleniumwire_options=seleniumwire_options
                )
                print(f"✅ Chrome created successfully (attempt {attempt + 1})")
                break
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    print(f"❌ All attempts failed")
                    return False
                else:
                    # Clean up any partial connections
                    try:
                        import psutil
                        # Kill any hanging chrome processes
                        for proc in psutil.process_iter(['pid', 'name']):
                            if 'chrome' in proc.info['name'].lower():
                                try:
                                    proc.terminate()
                                except:
                                    pass
                    except:
                        pass
        
        if not driver:
            print(f"❌ Failed to create driver")
            return False
        
        print(f"✅ Chrome with selenium-wire + SSL started!")
        
        # Test IP detection (EXACT COPY from temp driver logic)
        print(f"\n🌐 Testing IP detection with ipinfo.io/ip...")
        driver.get("https://ipinfo.io/ip")
        
        # Wait for the IP address to appear
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        try:
            ip_element = WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            detected_ip = ip_element.text.strip()
            
            print(f"🔍 Detected IP: {detected_ip}")
            
            # Basic IP validation
            import re
            if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", detected_ip):
                print(f"❌ Invalid IP format: {detected_ip}")
                driver.quit()
                return False
            
            # Check if it's the real IP (should NOT be)
            if detected_ip == "189.156.232.192":
                print(f"❌ CRITICAL: Still showing real IP - proxy failed!")
                print(f"   Expected: Proxy IP from DataImpulse")
                print(f"   Actual: {detected_ip} (real IP)")
                driver.quit()
                return False
            else:
                print(f"✅ SUCCESS: Proxy working - different IP detected!")
                print(f"   Real IP: 189.156.232.192")
                print(f"   Proxy IP: {detected_ip}")
                
                # Test multiple SSL sites for comprehensive verification
                ssl_test_sites = [
                    "https://www.google.com",
                    "https://www.linkedin.com",
                    "https://httpbin.org/headers"
                ]
                
                ssl_success_count = 0
                
                for test_url in ssl_test_sites:
                    print(f"\n🌐 Testing SSL security on {test_url}...")
                    try:
                        driver.get(test_url)
                        time.sleep(3)
                        
                        # Check what URL we actually ended up at
                        current_url = driver.current_url
                        page_title = driver.title
                        
                        print(f"🔍 Browser State Analysis:")
                        print(f"     Requested: {test_url}")
                        print(f"     Actual URL: {current_url}")
                        print(f"     Page Title: {page_title[:50]}...")
                        
                        # Check browser's actual security state via Chrome DevTools
                        try:
                            # Use Chrome DevTools Protocol to get the security state
                            security_info = driver.execute_script("""
                                // Try to access Chrome's security state through various methods
                                return new Promise((resolve) => {
                                    // Method 1: Check if TLS certificate is valid
                                    const hasSecureContext = window.isSecureContext;
                                    const protocol = window.location.protocol;
                                    
                                    // Method 2: Try to detect certificate errors via document properties
                                    const documentSecure = document.securityPolicy !== undefined;
                                    
                                    // Method 3: Check for mixed content warnings
                                    const hasMixedContent = document.querySelector('[data-security-warning]') !== null;
                                    
                                    resolve({
                                        hasSecureContext: hasSecureContext,
                                        protocol: protocol,
                                        documentSecure: documentSecure,
                                        hasMixedContent: hasMixedContent,
                                        userAgent: navigator.userAgent,
                                        url: window.location.href
                                    });
                                });
                            """)
                            
                            print(f"🔍 Security State Check:")
                            print(f"     Secure Context: {security_info.get('hasSecureContext', 'Unknown')}")
                            print(f"     Protocol: {security_info.get('protocol', 'Unknown')}")
                            print(f"     Document Secure: {security_info.get('documentSecure', 'Unknown')}")
                            
                        except Exception as e:
                            print(f"⚠️  Could not check security state via JavaScript: {e}")
                        
                        # Check for selenium-wire certificate issues
                        # The key indicator: if selenium-wire is doing MITM, the certificate chain will show "Selenium Wire CA"
                        try:
                            # Try to get certificate information
                            cert_info = driver.execute_cdp_cmd('Security.enable', {})
                            
                            # Get current security state
                            security_state = driver.execute_cdp_cmd('Security.getSecurityState', {})
                            security_level = security_state.get('securityState', 'unknown')
                            
                            print(f"🔍 Chrome Security State:")
                            print(f"     Security Level: {security_level}")
                            
                            # If security state is not 'secure', it means there are certificate issues
                            if security_level != 'secure':
                                print(f"❌ CHROME REPORTS INSECURE CONNECTION!")
                                print(f"     This means strikethrough HTTPS or 'Connection Not Secure'")
                                print(f"     Selenium-wire MITM certificates are not trusted")
                            else:
                                print(f"✅ Chrome reports secure connection")
                                ssl_success_count += 1
                                
                        except Exception as e:
                            print(f"⚠️  Could not check Chrome DevTools security state: {e}")
                            
                            # Fallback: assume SSL is broken if using selenium-wire
                            # Since selenium-wire always does MITM with untrusted certs
                            print(f"❌ ASSUMING SSL BROKEN:")
                            print(f"     Selenium-wire uses MITM with untrusted 'Selenium Wire CA'")
                            print(f"     This will always show 'Connection Not Secure' in browser")
                            print(f"     Certificate chain: Selenium Wire CA -> {test_url}")
                            
                    except Exception as e:
                        print(f"❌ Failed to load {test_url}: {e}")
                
                driver.quit()
                
                # Clean up
                try:
                    import shutil
                    shutil.rmtree(seleniumwire_cert_dir, ignore_errors=True)
                except:
                    pass
                
                # Determine SSL success
                total_sites = len(ssl_test_sites)
                ssl_success_rate = ssl_success_count / total_sites
                
                print(f"\n📊 SSL TEST RESULTS:")
                print(f"   Secure sites: {ssl_success_count}/{total_sites}")
                print(f"   Success rate: {ssl_success_rate:.1%}")
                
                if ssl_success_rate >= 0.75:  # 75% success rate
                    print(f"✅ SSL + PROXY COMBINATION SUCCESS!")
                    return True
                else:
                    print(f"❌ SSL issues detected - too many security warnings")
                    return False
                
        except Exception as e:
            print(f"❌ IP detection failed: {e}")
        
        driver.quit()
        return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if not DI_USERNAME or not DI_PASSWORD:
        print("❌ ERROR: DI_USERNAME or DI_PASSWORD not set!")
        exit(1)
    
    print("🔍 SELENIUM-WIRE + SSL COMBINED TEST")
    print("🎯 Testing exact configuration used in main scraper")
    print("=" * 60)
    
    success = test_seleniumwire_ssl_combined()
    
    print(f"\n" + "=" * 60)
    print("SELENIUM-WIRE + SSL COMBINED RESULT")
    print("=" * 60)
    
    if success:
        print(f"✅ SELENIUM-WIRE + SSL WORKS PERFECTLY!")
        print(f"   ✓ Proxy authentication successful")
        print(f"   ✓ IP properly changed to proxy IP")
        print(f"   ✓ SSL certificates properly handled")
        print(f"   ✓ LinkedIn loads securely")
        print(f"   ✓ Main scraper should work now")
    else:
        print(f"❌ SELENIUM-WIRE + SSL COMBINATION FAILED!")
        print(f"   ✗ Something wrong with main scraper configuration")
        print(f"   ✗ Need to debug specific issue")
        
        print(f"\n💡 DEBUGGING STEPS:")
        print(f"   1. Check if basic selenium-wire works (test_seleniumwire_basic.py)")
        print(f"   2. Check if direct proxy works (test_proxy.py)")
        print(f"   3. Compare working test vs main scraper config")
        print(f"   4. Check Chrome version compatibility")
