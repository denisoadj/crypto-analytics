from api_libraries import *

wci_key = 'X1rWsSfF4Rm2zgixd0vjT5cuDq4YuxCmijH'
wci_url = f"https://www.worldcoinindex.com/apiservice/v2getmarkets?key={wci_key}&fiat=btc"

response = r.get(wci_url)
wci_data = response.json()
wci_df = pd.DataFrame(wci_data['Markets'][0])


wci_df["log_volume_24h"] = np.log1p(wci_df["Volume_24h"])
wci_df["log_price"] = np.log1p(wci_df["Price"])

# unix time stamp
wci_df["Timestamp"] = pd.to_datetime(wci_df["Timestamp"], unit="s", utc=True)

wci_df["Datetime_local"] = wci_df["Timestamp"].dt.tz_convert("Europe/London").astype(str)

feature_cols =[
    'Label',
    'Name',
    'log_price',
    'log_volume_24h',
    'Datetime_local'
]

final_wci_df = wci_df[feature_cols]
print(final_wci_df.head())


