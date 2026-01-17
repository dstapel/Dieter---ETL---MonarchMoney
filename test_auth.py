#!/usr/bin/env python3
"""
Simple test script to verify MonarchMoney authentication works
"""
import asyncio
import getpass
import sys
import importlib

# Force clear any cached monarchmoney modules
modules_to_clear = [k for k in sys.modules.keys() if k.startswith('monarch')]
for module in modules_to_clear:
    del sys.modules[module]

from monarchmoney import MonarchMoney

async def test_auth():
    print("Testing MonarchMoney authentication...")
    
    # Check endpoints
    from monarchmoney.monarchmoney import MonarchMoneyEndpoints
    print(f"BASE_URL: {MonarchMoneyEndpoints.BASE_URL}")
    print(f"GraphQL: {MonarchMoneyEndpoints.getGraphQL()}")
    print(f"Login: {MonarchMoneyEndpoints.getLoginEndpoint()}")
    
    mm = MonarchMoney(timeout=30)
    
    try:
        email = input("Email: ")
        password = getpass.getpass("Password: ")
        
        print("Attempting login...")
        await mm.login(email, password)
        print("✅ Login successful!")
        
        print("Testing API call...")
        accounts = await mm.get_accounts()
        print(f"✅ Got {len(accounts.get('accounts', []))} accounts")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"Error type: {type(e)}")
        
        # Check if it's specifically a transport error
        if "525" in str(e):
            print("This is still a 525 Transport error - the endpoint might not be fully fixed")
        elif "401" in str(e) or "unauthorized" in str(e).lower():
            print("This is an authentication error - credentials might be wrong")
        else:
            print("This is a different type of error")

if __name__ == "__main__":
    asyncio.run(test_auth())