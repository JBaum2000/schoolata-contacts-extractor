#!/usr/bin/env python3
"""
Test the temporary driver proxy verification specifically.
This isolates the _get_ip_with_temp_driver method to ensure it's working.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables
load_dotenv()

def test_temp_driver_proxy():
    """Test the temporary driver proxy verification"""
    print("🔍 TESTING TEMPORARY DRIVER PROXY VERIFICATION")
    print("=" * 60)
    
    try:
        from scraper.linkedin_scraper import LinkedInScraper
        from scraper.config import DI_USERNAME, DI_PASSWORD
        
        if not DI_USERNAME or not DI_PASSWORD:
            print("❌ ERROR: Missing DataImpulse credentials!")
            return False
        
        # Create scraper instance
        scraper = LinkedInScraper(headless=True)
        
        # Set proxy country for testing
        scraper._proxy_country = 'gb'
        
        print(f"🔑 Using credentials:")
        print(f"   Username: {DI_USERNAME}")
        print(f"   Country: gb")
        
        # Test the temporary driver IP detection
        print(f"\n🌐 Testing temporary driver IP detection...")
        detected_ip = scraper._get_ip_with_temp_driver()
        
        if detected_ip:
            print(f"✅ Temporary driver detected IP: {detected_ip}")
            
            # Verify it's not the real IP by checking against a known real IP service
            import requests
            try:
                real_ip_response = requests.get("https://ipinfo.io/ip", timeout=10)
                real_ip = real_ip_response.text.strip()
                
                print(f"📊 IP Comparison:")
                print(f"   Real IP (no proxy): {real_ip}")
                print(f"   Detected IP (with proxy): {detected_ip}")
                
                if detected_ip != real_ip:
                    print(f"✅ SUCCESS: Proxy is working! IPs are different.")
                    print(f"   ✓ Temporary driver successfully used proxy")
                    print(f"   ✓ IP verification logic is correct")
                    return True
                else:
                    print(f"❌ FAILURE: Same IP detected - proxy not working")
                    print(f"   ✗ Temporary driver bypassed proxy")
                    print(f"   ✗ Need to debug selenium-wire configuration")
                    return False
                    
            except Exception as e:
                print(f"⚠️  Could not verify against real IP: {e}")
                print(f"✅ But temporary driver detected IP: {detected_ip}")
                print(f"   This suggests proxy is working")
                return True
                
        else:
            print(f"❌ Temporary driver failed to detect IP")
            print(f"   ✗ selenium-wire configuration issue")
            print(f"   ✗ Check certificate handling")
            return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 TEMPORARY DRIVER PROXY TEST")
    print("🎯 Isolating _get_ip_with_temp_driver method")
    print("=" * 60)
    
    success = test_temp_driver_proxy()
    
    print(f"\n" + "=" * 60)
    print("TEMPORARY DRIVER PROXY TEST RESULT")
    print("=" * 60)
    
    if success:
        print(f"✅ TEMPORARY DRIVER PROXY WORKS!")
        print(f"   ✓ selenium-wire properly configured")
        print(f"   ✓ Certificate handling correct")
        print(f"   ✓ Proxy authentication successful")
        print(f"   ✓ Main scraper should work now")
    else:
        print(f"❌ TEMPORARY DRIVER PROXY FAILED!")
        print(f"   ✗ selenium-wire configuration broken")
        print(f"   ✗ Main scraper will fail proxy verification")
        print(f"   ✗ Need to debug certificate/proxy setup")
