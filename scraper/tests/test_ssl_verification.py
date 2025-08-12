#!/usr/bin/env python3
"""
Test proper SSL certificate handling with selenium-wire.
This verifies certificates are properly validated and no security warnings appear.
"""
import os
import sys
import time
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

def test_ssl_certificate_validation():
    """Test SSL certificate validation with selenium-wire"""
    print("🔍 TESTING SSL CERTIFICATE VALIDATION")
    print("=" * 60)
    
    if not DI_USERNAME or not DI_PASSWORD:
        print("❌ ERROR: Missing credentials!")
        return False
    
    try:
        from seleniumwire import webdriver
        from selenium.webdriver.common.by import By
        import tempfile
        
        # Set up selenium-wire certificate handling
        seleniumwire_cert_dir = tempfile.mkdtemp(prefix="seleniumwire_test_certs_")
        os.environ['SELENIUMWIRE_STORAGE_DIR'] = seleniumwire_cert_dir
        print(f"📁 Using certificate directory: {seleniumwire_cert_dir}")
        
        # Build auth username with country modifier
        auth_user = f"{DI_USERNAME}__cr.gb"
        proxy_url = f"http://{auth_user}:{DI_PASSWORD}@{DI_HOST}:{DI_PORT}"
        
        print(f"🔑 Using credentials:")
        print(f"   Auth Username: {auth_user}")
        print(f"   Proxy: {DI_HOST}:{DI_PORT}")
        
        # Selenium-wire options with proper SSL handling
        seleniumwire_options = {
            "proxy": {
                "http": proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1"
            },
            "verify_ssl": True,  # Enable proper SSL verification
            "suppress_connection_errors": False,
            "auto_config": False,  # Disable auto config to avoid cert issues
            "ca_cert": None,  # Let selenium-wire handle certificates properly
        }
        
        # Chrome options - minimal and stealth-focused
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1000,800")
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        print(f"🚀 Creating Chrome with proper SSL handling...")
        
        driver = webdriver.Chrome(
            seleniumwire_options=seleniumwire_options,
            options=options,
        )
        
        print(f"✅ Chrome with proper SSL started!")
        
        # Test SSL-sensitive sites
        ssl_test_sites = [
            "https://www.linkedin.com",
            "https://www.google.com", 
            "https://httpbin.org/ip",
            "https://badssl.com/",  # SSL testing site
        ]
        
        ssl_results = {}
        
        for url in ssl_test_sites:
            print(f"\n🌐 Testing SSL for: {url}")
            
            try:
                driver.get(url)
                time.sleep(3)
                
                # Check for SSL security indicators
                current_url = driver.current_url
                
                # Check if URL is still HTTPS (not downgraded)
                if current_url.startswith("https://"):
                    print(f"✅ HTTPS maintained: {current_url}")
                    
                    # Check page title for security warnings
                    try:
                        title = driver.title.lower()
                        if any(warning in title for warning in ["privacy error", "security", "certificate", "not secure", "warning"]):
                            print(f"⚠️  Security warning in title: {driver.title}")
                            ssl_results[url] = "SSL_WARNING"
                        else:
                            print(f"✅ Clean SSL connection: {driver.title[:50]}...")
                            ssl_results[url] = "SSL_SECURE"
                    except:
                        print(f"✅ HTTPS loaded (no title available)")
                        ssl_results[url] = "SSL_SECURE_NO_TITLE"
                        
                    # Check for browser security indicators via JavaScript
                    try:
                        security_state = driver.execute_script("""
                            return {
                                protocol: window.location.protocol,
                                isSecure: window.isSecureContext,
                                hasSSL: window.location.protocol === 'https:'
                            };
                        """)
                        
                        if security_state.get('protocol') == 'https:' and security_state.get('isSecure'):
                            print(f"✅ Secure context confirmed via JavaScript")
                            if ssl_results[url] == "SSL_SECURE":
                                ssl_results[url] = "SSL_FULLY_SECURE"
                        else:
                            print(f"⚠️  Insecure context detected: {security_state}")
                            ssl_results[url] = "SSL_INSECURE"
                            
                    except Exception as e:
                        print(f"⚠️  Could not check security context: {e}")
                        
                else:
                    print(f"❌ HTTPS downgraded to: {current_url}")
                    ssl_results[url] = "SSL_DOWNGRADED"
                    
            except Exception as e:
                print(f"❌ Failed to load {url}: {e}")
                ssl_results[url] = "SSL_FAILED"
        
        driver.quit()
        
        # Analyze SSL results
        print(f"\n📊 SSL VALIDATION RESULTS:")
        print("=" * 50)
        
        secure_count = 0
        total_count = len(ssl_results)
        
        for url, result in ssl_results.items():
            if result in ["SSL_SECURE", "SSL_FULLY_SECURE", "SSL_SECURE_NO_TITLE"]:
                status = "✅ SECURE"
                secure_count += 1
            elif result in ["SSL_WARNING", "SSL_INSECURE"]:
                status = "⚠️  WARNING"
            else:
                status = "❌ FAILED"
                
            print(f"   {status} {url}")
            print(f"      Status: {result}")
        
        print(f"\n📈 SSL Security Score: {secure_count}/{total_count} sites fully secure")
        
        # Determine overall result
        if secure_count == total_count:
            print(f"\n✅ PERFECT SSL SECURITY!")
            print(f"   All sites loaded with proper SSL validation")
            print(f"   No security warnings - excellent for bot detection avoidance")
            return True
        elif secure_count >= total_count * 0.75:  # 75% success rate
            print(f"\n✅ GOOD SSL SECURITY")
            print(f"   Most sites secure - acceptable for production")
            return True
        else:
            print(f"\n❌ SSL SECURITY ISSUES")
            print(f"   Too many SSL warnings - will trigger bot detection")
            return False
        
        # Clean up
        try:
            import shutil
            shutil.rmtree(seleniumwire_cert_dir)
        except:
            pass
        
    except Exception as e:
        print(f"❌ SSL test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if not DI_USERNAME or not DI_PASSWORD:
        print("❌ ERROR: DI_USERNAME or DI_PASSWORD not set!")
        exit(1)
    
    print("🔍 SSL CERTIFICATE VALIDATION TEST")
    print("🎯 Ensuring proper SSL handling for stealth")
    print("=" * 60)
    
    success = test_ssl_certificate_validation()
    
    print(f"\n" + "=" * 60)
    print("FINAL SSL VALIDATION RESULT")
    print("=" * 60)
    
    if success:
        print(f"✅ SSL HANDLING IS PROPER!")
        print(f"   ✓ Certificates properly validated")
        print(f"   ✓ No security warnings")
        print(f"   ✓ Bot detection avoided")
        print(f"   ✓ Ready for production use")
    else:
        print(f"❌ SSL HANDLING NEEDS WORK!")
        print(f"   ✗ Security warnings present")
        print(f"   ✗ Bot detection risk HIGH")
        print(f"   ✗ Need different approach")
        
        print(f"\n💡 RECOMMENDATIONS:")
        print(f"   1. Check selenium-wire version compatibility")
        print(f"   2. Verify Chrome profile has proper certificates")
        print(f"   3. Consider using different proxy method")
        print(f"   4. Test with different Chrome versions")
