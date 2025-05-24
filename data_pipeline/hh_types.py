import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from config import CENSUS_BASE_URL, CENSUS_API_KEY, DB_CONNECTION_STRING
from utils.census_api import fetch_census_data

# create database connection
engine = create_engine(DB_CONNECTION_STRING)

# census vars
vars = [
        "B11001_001E",  # Total households
        "B11001_003E",  # Married-couple family
        "B11001_007E",  # Nonfamily households
        "B11001_008E",  # Householder living alone
        "B11003_002E",  # Married-couple family
        "B11003_003E",  # Married-couple family: With own children under 18 years
        "B11003_011E",  # Male householder: With own children under 18 years
        "B11003_017E"   # Female householder: With own children under 18 years
]

# fetch data
data = fetch_census_data(CENSUS_BASE_URL, vars, "state:*", CENSUS_API_KEY)

# process data 
header = data[0]
total_households_idx = header.index('B11001_001E')
married_couple_with_children_idx = header.index('B11003_003E')
married_couple_idx = header.index('B11003_002E')
single_parent_male_idx = header.index('B11003_011E')
single_parent_female_idx = header.index('B11003_017E')
living_alone_idx = header.index('B11001_008E')
other_nonfamily_alone_idx = header.index('B11001_008E')
other_nonfamily_idx = header.index('B11001_007E')

# calculate the probability distribution of household types
hh_types = []
for row in data[1:]:
    hh_types.append({        
        'state_code': row[header.index('state')],
        'married_with_children': int(row[married_couple_with_children_idx]) / int(row[total_households_idx]),                           # married_with_children = married_couple_with_children / total_households
        'married_no_children': (int(row[married_couple_idx]) - int(row[married_with_children_idx])) / int(row[total_households_idx]),   # married_no_children = married_couple_family - married_couple_family_with_own_children / total_households
        'single_parent_male': int(row[single_parent_male_idx]) / int(row[total_households_idx]),                                        # single_parent_male = male_householder_no_spouse_with_own_children / total_households
        'single_parent_female': int(row[single_parent_female_idx]) / int(row[total_households_idx]),                                    # single_parent_female = female_householder_no_spouse_with_own_children / total_households
        'living_alone': int(row[living_alone_idx]) / int(row[total_households_idx]),                                                    # living_alone = nonfamily_households_person_living_alone / total_households
        'other_nonfamily': (int(row[other_nonfamily_idx]) / int(row[other_nonfamily_alone_idx])) / int(row[total_households_idx]),      # other_nonfamily = nonfamily_households - nonfamily_households_person_living_alone / total_households
        'last_updated': datetime.now()
        })
# create pandas data frame and send to database
hh_types_df = pd.DataFrame(hh_types_df)
hh_types_df.to_sql('household_types', engine, if_exists='replace', index=False)

# randomly select a household type
# def select_household_type():
#     return random.choices(
#         list(prob_dist_of_hh_types.keys()),
#         weights=list(prob_dist_of_hh_types.values()),
#         k=1
#     )[0]
