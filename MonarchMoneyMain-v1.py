import asyncio
from monarchmoney import MonarchMoney, RequireMFAExpection

async def main():
    mm = MonarchMoney(
        timeout=3000,
    )
    await mm.login(
        email="dstapel@gmail.com",
        password="F0kk3nd13w3g3ldj13s!",
        save_session=False,
        use_saved_session=False,
        mfa_secret_key=""
    )
    accounts = await mm.get_accounts()
    print(accounts)
    await mm.close()

if __name__ == "__main__":
    asyncio.run(main())