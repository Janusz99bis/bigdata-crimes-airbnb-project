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
df.to_csv("dim_date.csv", index=False, header=True)

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

dim_LAW_CAT_CD_data = {
    "LAW_CAT_CD_key": [1, 2, 3, -1],
    "LAW_CAT_CD_code": ["F", "M", "V", "U"],
    "LAW_CAT_CD_desc": [
        "Felony",
        "Misdemeanor",
        "Violation",
        "Unknown",
    ],
}

dim_JURISDICTION_CODE_data = {
    "JURISDICTION_CODE_key": [0, 1, 2, 3],
    "JURISDICTION_CODE_desc": [
        "NYPD Patrol",
        "NYPD Transit",
        "NYPD Housing",
        "Non NYPD",
    ],
}

dim_ARREST_BORO_data = {
    "ARREST_BORO_key": [1, 2, 3, 4, 5, -1],
    "ARREST_BORO_code": ["B", "K", "M", "Q", "S", "U"],
    "ARREST_BORO_desc": [
        "Bronx",
        "Brooklyn",
        "Manhattan",
        "Queens",
        "Staten Island",
        "Unknown",
    ],
}

# df_police_nyc['AGE_GROUP'].unique()
dim_age_group_data = {
    "AGE_GROUP_key": [1, 2, 3, 4, 5, -1],
    "AGE_GROUP_desc": [
        "<18",
        "18-24",
        "25-44",
        "45-64",
        "65+",
        "Unknown",
    ],
}

# array(['Offender given a caution', 'Under investigation',
#        'Investigation complete; no suspect identified',
#        'Unable to prosecute suspect', nan, 'Awaiting court outcome',
#        'Further investigation is not in the public interest',
#        'Local resolution', 'Action to be taken by another organisation',
#        'Further action is not in the public interest'], dtype=object)
dim_outcome_category_data = {
    "outcome_category_key": [1, 2, 3, 4, 5, 6, 7, 8, 9, -1],
    "outcome_category_desc": [
        "Offender given a caution",
        "Under investigation",
        "Investigation complete; no suspect identified",
        "Unable to prosecute suspect",
        "Awaiting court outcome",
        "Further investigation is not in the public interest",
        "Local resolution",
        "Action to be taken by another organisation",
        "Further action is not in the public interest",
        "Unknown",
    ],
}

# array(['Other crime', 'Drugs', 'Criminal damage and arson',
#        'Theft from the person', 'Burglary', 'Other theft',
#        'Bicycle theft', 'Violence and sexual offences',
#        'Anti-social behaviour', 'Public order', 'Shoplifting', 'Robbery',
#        'Vehicle crime', 'Possession of weapons'], dtype=object)
dim_crime_type_data = {
    "crime_type_key": list(range(1, 15)) + [-1],
    "crime_type_desc": [
        "Other crime",
        "Drugs",
        "Criminal damage and arson",
        "Theft from the person",
        "Burglary",
        "Other theft",
        "Bicycle theft",
        "Violence and sexual offences",
        "Anti-social behaviour",
        "Public order",
        "Shoplifting",
        "Robbery",
        "Vehicle crime",
        "Possession of weapons",
        "Unknown",
    ],
}

dim_crime_type_df = pd.DataFrame(dim_crime_type_data)
dim_crime_type_df.to_csv("dim_crime_type.csv", index=False, header=True)

dim_outcome_category_df = pd.DataFrame(dim_outcome_category_data)
dim_outcome_category_df.to_csv("dim_outcome_category.csv", index=False, header=True)

dim_host_response_time_df = pd.DataFrame(dim_host_response_time_data)
dim_host_response_time_df.to_csv("dim_host_response_time.csv", index=False, header=True)

dim_room_type_df = pd.DataFrame(dim_room_type_data)
dim_room_type_df.to_csv("dim_room_type.csv", index=False, header=True)

dim_LAW_CAT_CD_df = pd.DataFrame(dim_LAW_CAT_CD_data)
dim_LAW_CAT_CD_df.to_csv("dim_LAW_CAT_CD.csv", index=False, header=True)

dim_JURISDICTION_CODE_df = pd.DataFrame(dim_JURISDICTION_CODE_data)
dim_JURISDICTION_CODE_df.to_csv("dim_JURISDICTION_CODE.csv", index=False, header=True)

dim_ARREST_BORO_df = pd.DataFrame(dim_ARREST_BORO_data)
dim_ARREST_BORO_df.to_csv("dim_ARREST_BORO.csv", index=False, header=True)

dim_age_group_df = pd.DataFrame(dim_age_group_data)
dim_age_group_df.to_csv("dim_age_group.csv", index=False, header=True)
