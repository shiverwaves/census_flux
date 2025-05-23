"""
Main script for the Census household and family type data pipeline.
Fetches ACS data from Census API and loads it into the database.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

from config import BASE_URL, API_KEY, DB_CONNECTION_STRING
from utils.census_api import fetch_census_data
from utils.data_processing import process_data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/census_pipeline.log'
)
logger = logging.getLogger(__name__)

def setup_database():
    """Create database connection and set up required tables"""
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        logger.info("Database connection established")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def fetch_and_store_states(engine):
    """Fetch state data and store in database"""
    logger.info("Fetching state data")
    vars = ["NAME"]
    
    try:
        # Fetch state data
        data = fetch_census_data(BASE_URL, vars, "state:*", API_KEY)
        
        # Process data
        header = data[0]
        states_data = []
        for row in data[1:]:
            states_data.append({        
                'state_code': row[header.index('state')],
                'state_name': row[header.index('NAME')],
                'last_updated': datetime.now()
            })
            
        # Create DataFrame and update database
        states_df = pd.DataFrame(states_data)
        states_df.to_sql('states', engine, if_exists='replace', index=False)
        logger.info(f"Stored {len(states_data)} states in database")
        return states_df
    except Exception as e:
        logger.error(f"Error fetching state data: {str(e)}")
        raise

def fetch_and_store_household_type(engine):
    """Fetch household type data and store in database"""
    logger.info("Fetching household type data")
    vars = [
        "B11001_001E",  # Total households
        "B11001_003E",  # Married-couple family
        "B11001_005E",  # Male householder, no spouse present
        "B11001_006E",  # Female householder, no spouse present
        "B11001_008E",  # Householder living alone
        "B11001_009E"   # Householder not living alone
    ]
    
    try:
        # Fetch data
        data = fetch_census_data(BASE_URL, vars, "state:*", API_KEY)
        
        # Process data
        header = data[0]
        total_households_idx = header.index('B11001_001E')
        married_couple_idx = header.index('B11001_003E')
        male_householder_idx = header.index('B11001_005E')
        female_householder_idx = header.index('B11001_006E')
        living_alone_idx = header.index('B11001_008E')
        not_alone_hh_idx = header.index('B11001_009E')
        
        hh_type_data = []
        for row in data[1:]:
            hh_type_data.append({        
                'state_code': row[header.index('state')],
                'total_households': int(row[total_households_idx]),
                'married_couple': int(row[married_couple_idx]),
                'male_householder': int(row[male_householder_idx]),
                'female_householder': int(row[female_householder_idx]),
                'living_alone': int(row[living_alone_idx]),
                'not_living_alone': int(row[not_alone_hh_idx]),
                'last_updated': datetime.now()
            })
            
        # Create DataFrame and update database
        hh_type_df = pd.DataFrame(hh_type_data)
        hh_type_df.to_sql('household_type', engine, if_exists='replace', index=False)
        logger.info(f"Stored household type data for {len(hh_type_data)} states")
        return hh_type_df
    except Exception as e:
        logger.error(f"Error fetching household type data: {str(e)}")
        raise

def fetch_and_store_family_type(engine):
    """Fetch family type data and store in database"""
    logger.info("Fetching family type data")
    vars = [
        "B11003_003E",  # Married-couple family: With own children under 18 years
        "B11003_011E",  # Male householder: With own children under 18 years
        "B11003_017E"   # Female householder: With own children under 18 years
    ]
    
    try:
        # Fetch data
        data = fetch_census_data(BASE_URL, vars, "state:*", API_KEY)
        
        # Process data
        header = data[0]
        married_with_children_idx = header.index('B11003_003E')
        male_with_children_idx = header.index('B11003_011E')
        female_with_children_idx = header.index('B11003_017E')
        
        fam_type_data = []
        for row in data[1:]:
            fam_type_data.append({        
                'state_code': row[header.index('state')],
                'married_with_children': int(row[married_with_children_idx]),
                'male_with_children': int(row[male_with_children_idx]),
                'female_with_children': int(row[female_with_children_idx]),
                'last_updated': datetime.now()
            })
            
        # Create DataFrame and update database
        fam_type_df = pd.DataFrame(fam_type_data)
        fam_type_df.to_sql('family_type', engine, if_exists='replace', index=False)
        logger.info(f"Stored family type data for {len(fam_type_data)} states")
        return fam_type_df
    except Exception as e:
        logger.error(f"Error fetching family type data: {str(e)}")
        raise

# def calculate_probabilities(engine):
#     """Calculate household and family type probabilities and store in database"""
#     logger.info("Calculating household and family type probabilities")
    
#     # Define SQL to create the target table if it doesn't exist
#     create_table_sql = """
#     CREATE TABLE IF NOT EXISTS household_family_type_probabilities (
#         state_code VARCHAR(10),
#         state_name VARCHAR(100),
#         married_with_children FLOAT,
#         married_no_children FLOAT,
#         single_parent_male FLOAT,
#         single_parent_female FLOAT,
#         single_person FLOAT,
#         other_nonfamily FLOAT,
#         other_family_male FLOAT,
#         other_family_female FLOAT,
#         last_updated DATETIME,
#         PRIMARY KEY (state_code)
#     );
#     """
    
#     # SQL to clear existing data
#     delete_data_sql = "DELETE FROM household_family_type_probabilities;"
    
#     # SQL to calculate probabilities and insert into the target table
#     insert_probabilities_sql = """
#     INSERT INTO household_family_type_probabilities (
#         state_code,
#         state_name,
#         married_with_children,
#         married_no_children,
#         single_parent_male,
#         single_parent_female,
#         single_person,
#         other_nonfamily,
#         other_family_male,
#         other_family_female,
#         last_updated
#     )
#     SELECT 
#         h.state_code,
#         s.state_name,
#         f.married_with_children / h.total_households AS married_with_children,
#         (h.married_couple - f.married_with_children) / h.total_households AS married_no_children,
#         f.male_with_children / h.total_households AS single_parent_male,
#         f.female_with_children / h.total_households AS single_parent_female,
#         h.living_alone / h.total_households AS single_person,
#         h.not_living_alone / h.total_households AS other_nonfamily,
#         (h.male_householder - f.male_with_children) / h.total_households AS other_family_male,
#         (h.female_householder - f.female_with_children) / h.total_households AS other_family_female,
#         NOW() AS last_updated
#     FROM 
#         household_type h
#     JOIN 
#         family_type f ON h.state_code = f.state_code
#     JOIN 
#         states s ON h.state_code = s.state_code
#     WHERE 
#         h.total_households > 0;
#     """
    
#     try:
#         with engine.connect() as connection:
#             # Create the table if it doesn't exist
#             connection.execute(text(create_table_sql))
#             connection.commit()
            
#             # Clear existing data
#             connection.execute(text(delete_data_sql))
#             connection.commit()
            
#             # Calculate probabilities and insert into the table
#             connection.execute(text(insert_probabilities_sql))
#             connection.commit()
            
#             # Verify the results
#             result_count = connection.execute(text("SELECT COUNT(*) FROM household_family_type_probabilities")).scalar()
#             logger.info(f"Calculated probabilities for {result_count} states")
#             return result_count
#     except Exception as e:
#         logger.error(f"Error calculating probabilities: {str(e)}")
#         raise

def run_pipeline():
    """Main function to run the full data pipeline"""
    logger.info("Starting Census household and family type data pipeline")
    
    try:
        # Setup database connection
        engine = setup_database()
        
        # Fetch and store state data
        fetch_and_store_states(engine)
        
        # Fetch and store household type data
        fetch_and_store_household_type(engine)
        
        # Fetch and store family type data
        fetch_and_store_family_type(engine)
        
        # # Calculate probabilities
        # num_states = calculate_probabilities(engine)
        
        # logger.info(f"Successfully completed Census data pipeline for {num_states} states")
        return True
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    run_pipeline()
