from utils import clob_client, market, order, allowance, positions, balance, market_listener
import json

from py_clob_client.clob_types import BalanceAllowanceParams
from py_clob_client.order_builder.constants import BUY, SELL

# run whenever needed
# allowance.set_allowances()
# clob_client.generate_api_keys()
print(balance.fetch_balance())
# print(allowance.fetch_allowance())

bot = clob_client.create_client()
# order.cancel_all_orders(bot)

mkt = market.get_single_byName(bot, "Will Ontario resume electricity surcharge to the U.S. by next Friday?")
print(json.dumps(mkt, indent = 4))
# order.create_and_submit_order(bot, mkt['tokens'][0]['token_id'], BUY, 0.80, 5)
# order.create_and_submit_order(bot, mkt['tokens'][1]['token_id'], BUY, 0.15, 5)

# print(f'Sampling Markets: {len(sampling_markets)}\nActive Markets: {activeMarkets}\nClosed Markets: {closedMarkets}\n')

# print(json.dumps(sampling_markets, indent = 4))

# markets = market.get_all(bot)
# print(json.dumps(markets[55], indent = 4))


# # for elem in markets:
# #     if 'win the Romanian presidential election?' in elem['question']:
# #         print(f'{json.dumps(elem, indent =4)}\n\n')

# # print(market.fetch_all(bot))

# demo_market1 = 'Will Manchester City win the UEFA Champions League?'
# demo_market_obj1 = market.fetch_single(bot, demo_market1)

# demo_market2 = 'Will Manchester City win the UEFA Champions League?'
# demo_market_obj2 = market.get_single_byName(bot, demo_market2)

# print(json.dumps(demo_market_obj2, indent = 4))
# print(f'Looking into event: \'{demo_market2}\'\n')

# print(json.dumps(demo_market_obj2, indent = 4))
# print(f'Looking into event: \'{demo_market2}\'\n')

# order_type = input(f'Choose order type (Buy / Sell): ')
# choice = input('Choose token (Yes / No): ')

# # print(f'Filling order in...\n{demo_market} -> {order_type} {choice}')

# # token = next((item for item in demo_market_obj1['tokens'] if item.get('outcome') == 'Yes'), None)
# # order.create_and_submit_order(bot, token['token_id'], BUY, 0.150, 5)

# token = next((item for item in demo_market_obj2['tokens'] if item.get('outcome') == 'Yes'), None)
# order.create_and_submit_order(bot, token['token_id'], BUY, 0.10, 5)

# # order.cancel_all_orders(bot)

# # token_id = '45714870634090908403813747458214625542376052548606303175331201110938821302832'

# # signed_order = order.create_buy_order(bot_client, 0.10, 1, token_id)

# # print(signed_order.order)
# # resp = order.post_order(bot_client, signed_order)
# # print(resp)

# # markets_total = market.fetch_all(bot_client)
# # print(f'Total Markets fetched by API: {len(markets_total)}\n')
# # daily_possible_rewards = market.total_possible_rewards(bot_client)

# # print(f'Max rewards today:\nNormal rate (1.5%): {daily_possible_rewards / 66} $\nGood (5%) rate: {daily_possible_rewards / 20} $\nTotal (100%) rate: {daily_possible_rewards} $\n')
