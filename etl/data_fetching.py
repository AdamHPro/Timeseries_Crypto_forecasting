import investpy

df = investpy.get_crypto_historical_data(
    crypto='bitcoin', from_date='01/01/2016', to_date='01/01/2021')
print(df.head())
