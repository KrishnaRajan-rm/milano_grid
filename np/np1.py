import pandas as pd 

dataset1= pd.read_csv("../dataset/raw/sms-call-internet-mi-2013-11-01.csv")
# dataset2= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-02.csv")
# dataset3= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-03.csv")
# dataset4= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-04.csv")
# dataset5= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-05.csv")
# dataset6= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-06.csv")
# dataset7= pd.read_csv("dataset\\raw\\sms-call-internet-mi-2013-11-07.csv")


print(dataset1.head())
print(dataset1.shape)
print(dataset1.dtypes)
print(dataset1.columns)

df= dataset1.rename(columns={
    'datetime':'timestamp',
    'CellID':'grid_id',
    'countrycode':'country_code',
    'smsin':'sms_in',
    'smsout':'sms_out',
    'callin':'call_in',
    'callout':'call_out',
    'internet':'internet'}).copy()

print(df.head())

df["timestamp"]=pd.to_datetime(df["timestamp"],errors='coerce')

print(df["timestamp"].dtype)

unique_timestamps=df['timestamp'].dropna().drop_duplicates().sort_values()

diff_timestamps=unique_timestamps.diff()

consicutive_intervals= (diff_timestamps== pd.Timedelta(hours=1)).sum()

print("Cadence :", (len(unique_timestamps)==24 and consicutive_intervals==23))

df["date"]=df["timestamp"].dt.date
df["hour"]=df["timestamp"].dt.hour
df["day_of_week"]=df["timestamp"].dt.day_name()

print(df.columns)

columns=df.columns.to_list()

print("null values:")
for column in columns:
    print(f"{column}: {df[column].isnull().sum()}")

print("duplicate values:")
print(df.duplicated().sum())

print("Negative values:")
for column in columns:
    if df[column].dtypes == int:
        print(f"{column}: {(df[column]<0).sum()}")

graincolumns=[
    'timestamp',
    'grid_id',
    'country_code'
]

grain_count=df.groupby(graincolumns,dropna=False).size()
duplicate_grain_count=grain_count[grain_count>1].sum()
print("Duplicate grain count:", duplicate_grain_count)

group_country_code=df.groupby(['country_code', 'hour'], dropna=False)["country_code"].nunique()
print(group_country_code)

df["total_sms"]=df["sms_in"].fillna(0)+df["sms_out"].fillna(0)
df["total_call"]=df["call_in"].fillna(0)+df["call_out"].fillna(0)
df["total_activity"]=df["total_sms"].fillna(0)+df["total_call"].fillna(0)+df["internet"].fillna(0)
unique_grids=df["grid_id"].nunique()
time_min=df["timestamp"].min()
time_max=df["timestamp"].max()
country_code_categories = df["country_code"].nunique()
hourly_activity = (df.groupby("hour")["total_activity"].sum().sort_values(ascending = False))
busiest_hour = hourly_activity.index[0]
 
busiest_hour_activity = hourly_activity.iloc[0]
 
grid_activity = (df.groupby("grid_id")["total_activity"].sum().sort_values(ascending = False))
print(grid_activity)
 
busiest_grid = grid_activity.index[0]
 
busiest_grid_activity = grid_activity.iloc[0]









