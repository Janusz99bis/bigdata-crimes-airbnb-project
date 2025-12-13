import pandas as pd

start_date = "2000-01-01"
end_date = "2030-12-31"

dates = pd.date_range(start=start_date, end=end_date)
df = pd.DataFrame(dates, columns=["full_date"])

df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
df["date_str"] = df["full_date"].dt.strftime("%Y-%m-%d")
df["year"] = df["full_date"].dt.year
df["month"] = df["full_date"].dt.month
df["day"] = df["full_date"].dt.day
df["day_name"] = df["full_date"].dt.day_name()
df["is_weekend"] = df["full_date"].dt.dayofweek.isin([5, 6])

df = df.drop(columns=["full_date"])
df.to_csv("dim_date_load.csv", index=False, header=True)

dim_host_response_time_data = {
    "response_time_key": [1, 2, 3, 4, 0, -1],
    "response_time_desc": [
        "within an hour",
        "within a few hours",
        "within a day",
        "a few days or more",
        "no info",
        "Unknown",
    ],
}
dim_host_response_time_df = pd.DataFrame(dim_host_response_time_data)
dim_host_response_time_df.to_csv("dim_host_response_time.csv", index=False, header=True)

dim_room_type_data = {
    "room_type_key": [1, 2, 3, 4, -1],
    "room_type": [
        "Entire home/apt",
        "Hotel room",
        "Private room",
        "Shared room",
        "Unknown",
    ],
}
dim_room_type_df = pd.DataFrame(dim_room_type_data)
dim_room_type_df.to_csv("dim_room_type.csv", index=False, header=True)
