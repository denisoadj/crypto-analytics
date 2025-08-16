import pandas as pd
import numpy as np
import requests as r

# WORLD COIN INDEX

wci_key = 'X1rWsSfF4Rm2zgixd0vjT5cuDq4YuxCmijH'
wci_url = f"https://www.worldcoinindex.com/apiservice/v2getmarkets?key={wci_key}&fiat=btc"

response = r.get(wci_url)
data = response.json()
wci_df = pd.DataFrame(data['Markets'][0])
print(wci_df.tail()) #24h volume data and price for each coin

#
