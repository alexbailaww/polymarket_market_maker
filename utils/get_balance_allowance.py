import client

from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

# client.generate_creds()

bot_client = client.Client()
collateral = bot_client.get_balance_allowance(
        params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
)
print(collateral)

yes = bot_client.get_balance_allowance(
    params=BalanceAllowanceParams(
        asset_type=AssetType.CONDITIONAL,
        token_id="52114319501245915516055106046884209969926127482827954674443846427813813222426",
    )
)
print(yes)

no = bot_client.get_balance_allowance(
    params=BalanceAllowanceParams(
        asset_type=AssetType.CONDITIONAL,
        token_id="71321045679252212594626385532706912750332728571942532289631379312455583992563",
    )
)
print(no)