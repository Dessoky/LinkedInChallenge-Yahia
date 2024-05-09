# %%
import pandas as pd
import requests
from unidecode import unidecode
from datetime import datetime, timedelta

# %%
response = requests.get("https://restcountries.com/v3.1/all")
country_data = response.json()
capital_to_country = {
    country["capital"][0].upper(): country["name"]["common"].upper()
    for country in country_data
    if "capital" in country
}

raw_data = pd.read_excel("My port\MyPoert AUG sheet1.xlsx", sheet_name=None)
all_dfs_list = []
for dataframe in raw_data.keys():
    raw_data[dataframe] = raw_data[dataframe].iloc[:, :2]
    raw_data[dataframe].columns = ["Trip", "PriceOffer"]
    raw_data[dataframe]["Country"] = dataframe.upper().strip()
    all_dfs_list.append(raw_data[dataframe])

df = pd.concat(all_dfs_list, ignore_index=True)

# %%
df["Trip"] = df["Trip"].str.upper().str.strip()
df["PriceOffer"] = df["PriceOffer"].str.upper()
df["PriceOffer"] = (
    df["PriceOffer"].str.replace("\n", " ").str.replace(":", " ").str.strip()
)
df.fillna(False, inplace=True)


# %%
def pattern_extractor(df, column_name, pattern) -> dict:
    # pattern = r"FROM\s?(?P<From>.+?)\s?TO\s?(?P<To>.+)"
    extracted_data = df[column_name].str.extract(pattern)
    for column in extracted_data.columns:
        extracted_data[column] = extracted_data[column].str.strip()
    return pd.concat([df, extracted_data], axis=1)


# %%
def trip_type_extractor(price_offer: str) -> bool | str:
    if "ONE" in price_offer:
        return "O"
    elif "ROUND" in price_offer:
        return "R"
    return False


# %%
raw_data_dict = {}
for ind in df.index:
    trip = df.loc[ind, "Trip"]
    price_offer = df.loc[ind, "PriceOffer"]
    country = df.loc[ind, "Country"]
    if trip:
        raw_data_dict[ind] = {
            "Trip": trip,
            "PriceOffer": price_offer,
            "Country": country,
            "PriceOfferSplitted": False,
        }

        _last_index = ind
    else:
        if price_offer:
            raw_data_dict[_last_index]["PriceOffer"] = (
                raw_data_dict[_last_index]["PriceOffer"] + " " + str(price_offer)
            )
            raw_data_dict[_last_index]["PriceOfferSplitted"] = True

pre_final_df = pd.DataFrame.from_dict(raw_data_dict, orient="index").reset_index(
    drop=True
)
pre_final_df = pattern_extractor(
    pre_final_df,
    "Trip",
    r"(?:FROM|FRM)\s?(?P<From>.+?)\s?TO\s?(?P<To>.+)",
)
pre_final_df = pattern_extractor(
    pre_final_df,
    "PriceOffer",
    r"(?P<CurrencyType>[a-zA-Z]{3})\s*(?P<TripPrice>\d+)",
)

pre_final_df["TripType"] = pre_final_df["PriceOffer"].apply(
    lambda x: trip_type_extractor(x)
)

# TODO: Handle incase there is null values in From or To
pre_final_df["From"] = pre_final_df["From"].apply(unidecode)
pre_final_df["To"] = pre_final_df["To"].apply(unidecode)
# %%
countries_df = pd.read_excel("My port\\Countries IDs.xlsx")
countries_df["Country"] = countries_df["Country"].str.upper()
# TODO: make sure tha this DF always contain unique country name and ID
countries_dict = countries_df.set_index("Country")["ID"].to_dict()
# %%
pre_final_df["CountryID"] = pre_final_df["Country"].apply(
    lambda x: countries_dict.get(x, "Not Available")
)

# %%

# Fixing data entry of Country capital instead of the Country it self.
pre_final_df.loc[pre_final_df["CountryID"] == "Not Available", "Country"] = (
    pre_final_df.loc[pre_final_df["CountryID"] == "Not Available"]["Country"].apply(
        lambda x: capital_to_country[x]
    )
)

pre_final_df["CountryID"] = pre_final_df["Country"].apply(
    lambda x: countries_dict.get(x, "Not Available")
)


# %%
ports_df = pd.read_excel("My port\\Ports_ceties_codes_and_IDs.xlsx")
# TODO: handle in case we find a city with 2 port names duplicated subset by City & port name
# TODO: and always check if the ID column is unique

ports_df["City"] = ports_df["City"].str.upper()
ports_df["City"] = ports_df["City"].apply(unidecode)
ports_df["Port Name"] = ports_df["Port Name"].str.upper()
ports_df["Port Name"] = ports_df["Port Name"].apply(unidecode)
ports_df["Code"] = ports_df["Code"].str.upper()

duplicated_cities = ports_df[ports_df.duplicated(subset=["City"])]["City"].unique()
ports_dict = ports_df.set_index("Port Name").to_dict(orient="index")

ports_by_city_dict = (
    ports_df[~ports_df["City"].isin(duplicated_cities)]
    .set_index("City")
    .to_dict(orient="index")
)


# %%
def port_id_and_code_extractor(port_name):
    port_id = ports_dict.get(port_name, {}).get("ID", "N.A.")
    port_code = ports_dict.get(port_name, {}).get("Code", "N.A.")

    if port_id == "N.A.":
        if port_name in duplicated_cities:
            return {
                "port_id": "duplicated city",
                "port_code": "duplicated city",
            }

        port_id = ports_by_city_dict.get(port_name, {}).get("ID", "N.A.")
        port_code = ports_by_city_dict.get(port_name, {}).get("Code", "N.A.")

    return {
        "port_id": port_id,
        "port_code": port_code,
    }


# %%
pre_final_df["[From] Port ID"] = pre_final_df["From"].apply(
    lambda x: port_id_and_code_extractor(x).get("port_id")
)
pre_final_df["[From] Port Code"] = pre_final_df["From"].apply(
    lambda x: port_id_and_code_extractor(x).get("port_code")
)
pre_final_df["[To] Port ID"] = pre_final_df["To"].apply(
    lambda x: port_id_and_code_extractor(x).get("port_id")
)
pre_final_df["[To] Port Code"] = pre_final_df["To"].apply(
    lambda x: port_id_and_code_extractor(x).get("port_code")
)

# TODO: handle the not available cases in FROM, TO columns
# decided to handle it in a seprate produced report contains the country name and the From, To names to help detect the data entry mistakes,

# %%
pre_final_df["URL"] = (
    pre_final_df["[From] Port Code"].astype(str)
    + "-"
    + pre_final_df["[To] Port Code"].astype(str)
    + "-"
    + pre_final_df["TripType"].astype(str)
).str.lower()

# %%
def error_column_extractor(url:str) -> str:
    url = url.split("-")
    error_string = ""
    from_string = url[0]
    to_string = url[1]
    trip_type = url[2]

    if from_string == "n.a.":
        error_string += "* There is an error in [From] column"
    if to_string == "n.a.":
        error_string += "* There is an error in [To] column"
    if trip_type == 'false':
        error_string += "* Maybe the Offer column doesn't containt the Trip Type (One Way or Round)"

    return error_string
# %%
# Errors to be reported.
# TODO: enhance the error categories
errors_df = pre_final_df[
    (pre_final_df["URL"].str.contains("n.a."))
    | (pre_final_df["URL"].str.contains("duplicated city"))
].copy()

errors_df['Error Type'] = errors_df['URL'].apply(lambda x: error_column_extractor(x))
if not errors_df.empty:
    errors_df[['Country', 'CountryID', 'From', 'To', 'Error Type']].to_excel(f'Results\\Unhandled_Errors_({errors_df.shape[0]})_in_Raw_Data.xlsx', index=False)

# %%
# Final df
final_df = pre_final_df[
    ~(
        (pre_final_df["URL"].str.contains("n.a."))
        | (pre_final_df["URL"].str.contains("duplicated city"))
    )
]

# %%
# Offers Template
offers_template_df = final_df[
    ["[From] Port ID", "[To] Port ID", "TripType", "URL"]
].copy()
offers_template_df["publish_date"] = datetime.today().date()
offers_template_df["expir_date"] = datetime.today().date() + timedelta(days=34)

offers_template_df.rename(
    {
        "[From] Port ID": "port_from_id",
        "[To] Port ID": "port_to_id",
        "TripType": "trip_type",
        "URL": "url",
    },
    axis=1,
    inplace=True,
)
offers_template_df_unique = offers_template_df.copy().drop_duplicates(
    subset=["port_from_id", "port_to_id"]
)
# %%
# Producing first requirement (Offers Template)
offers_template_df_unique.to_excel("Results\\offers_template.xlsx", index=False)


# %%
def price_maker(price: str) -> str:
    arabic_numeral_mapping = {
        "0": "۰",
        "1": "۱",
        "2": "۲",
        "3": "۳",
        "4": "٤",
        "5": "٥",
        "6": "٦",
        "7": "۷",
        "8": "۸",
        "9": "۹",
    }
    arabic_price = "".join(arabic_numeral_mapping[digit] for digit in price)

    price_template = {
        "en": f"{price}",
        "ar": f"{arabic_price}",
        "gr": f"{price}",
        "it": f"{price}",
        "cz": f"{price}",
        "fr": f"{price}",
        "sk": f"{price}",
    }
    return str(price_template)


# %%
offer_content_df = final_df[["URL", "CountryID", "TripPrice"]].copy()
offer_content_df["price"] = offer_content_df["TripPrice"].apply(
    lambda x: price_maker(x)
)

offer_content_df.rename(
    {
        "URL": "offer_url",
        "CountryID": "country_id",
    },
    axis=1,
    inplace=True
)
# %%
# Producing the 2nd requirement (offer_content file)
offer_content_df[["offer_url", "country_id", "price"]].to_excel("Results\\offers_content.xlsx", index=False)
