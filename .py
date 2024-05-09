# %%
import pandas as pd

raw_data = pd.read_excel('My port\MyPoert AUG sheet1.xlsx', sheet_name=None)
all_dfs_list = []
for dataframe in raw_data.keys():
    raw_data[dataframe] = raw_data[dataframe].iloc[:, :2]
    raw_data[dataframe].columns = ["Trip", "Price Offer"]
    raw_data[dataframe]['Type'] = dataframe
    all_dfs_list.append(raw_data[dataframe])

df = pd.concat(all_dfs_list, ignore_index=True)
df.dropna(inplace=True)
df['Trip'] = df['Trip'].str.upper().str.strip()
df['Price Offer'] = df['Price Offer'].str.upper()
df['Price Offer'] = df['Price Offer'].str.replace("\n", " ").str.replace(":", " ").str.strip()

# %%
trip_from_to_pattern = r'FROM\s+(?P<From>.+?)\s+TO\s+(?P<To>.+)'
extracted_from_to = df['Trip'].str.extract(trip_from_to_pattern)

df = pd.concat([df, extracted_from_to], axis=1)

# %%
df['Trip Type'] = df['Price Offer'].apply(lambda x: "O" if "ONE" in x else "R")
# %%
currency_and_price_pattern = r'(?P<CurrencyType>[a-zA-Z]{3})\s+(?P<TripPrice>\d+)'
extracted_currency_and_price = df['Price Offer'].str.extract(currency_and_price_pattern)
df = pd.concat([df, extracted_currency_and_price], axis=1)

# %%
