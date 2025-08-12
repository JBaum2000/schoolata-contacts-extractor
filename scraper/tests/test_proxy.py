import requests
import os
from dotenv import load_dotenv

# Load your .env file
load_dotenv()

# Get credentials from environment
DI_USERNAME = os.getenv("DI_USERNAME")
DI_PASSWORD = os.getenv("DI_PASSWORD")
DI_HOST = os.getenv("DI_HOST", "pr.dataimpulse.com")
DI_PORT = os.getenv("DI_PORT", "823")

# Test with different country configurations
test_configs = [
    {"country": None, "label": "No country specified"},
    {"country": "gb", "label": "United Kingdom"},
    {"country": "us", "label": "United States"},
]

print("Testing Data Impulse proxy connection...")
print(f"Host: {DI_HOST}:{DI_PORT}")
print(f"Username: {DI_USERNAME}")
print("-" * 50)

for config in test_configs:
    # Build auth username with country modifier if specified
    auth_user = DI_USERNAME
    if config["country"]:
        # DataImpulse uses __cr format for country targeting
        auth_user = f"{DI_USERNAME}__cr.{config['country']}"
    
    # Create proxy URL
    proxy_url = f"http://{auth_user}:{DI_PASSWORD}@{DI_HOST}:{DI_PORT}"
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    
    print(f"\nTesting: {config['label']}")
    print(f"Auth user: {auth_user}")
    
    try:
        # Test IP detection
        response = requests.get("https://ipinfo.io/json", proxies=proxies, timeout=10)
        data = response.json()
        
        print(f"✅ Success!")
        print(f"   IP: {data.get('ip')}")
        print(f"   Location: {data.get('city')}, {data.get('region')}, {data.get('country')}")
        print(f"   ISP: {data.get('org')}")
        
    except requests.exceptions.ProxyError as e:
        print(f"❌ Proxy Error: Authentication failed or proxy not reachable")
        print(f"   Details: {str(e)}")
        
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection Error: Cannot connect to proxy")
        print(f"   Details: {str(e)}")
        
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {str(e)}")

print("\n" + "=" * 50)
print("Test complete!")
print("\nIf all tests failed, please check:")
print("1. Your Data Impulse credentials are correct")
print("2. Your account is active and has available bandwidth")
print("3. Your firewall/antivirus isn't blocking the connection")
print("4. The proxy host/port are correct (pr.dataimpulse.com:823)")