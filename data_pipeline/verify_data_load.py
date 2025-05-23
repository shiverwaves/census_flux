"""
Verify the data loaded into the database
"""
import pandas as pd
import logging
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/verify_data_load.log'
)
logger = logging.getLogger(__name__)

def verify_data_load():
    """
    Verify the data was loaded correctly and generate a report
    """
    logger.info("Starting data load verification")
    
    try:
        # Connect to database
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Verify states table
        with engine.connect() as conn:
            state_count = conn.execute(text("SELECT COUNT(*) FROM states")).scalar()
            logger.info(f"Found {state_count} states in the database")
            
            # Verify household_type table
            hh_count = conn.execute(text("SELECT COUNT(*) FROM household_type")).scalar()
            logger.info(f"Found {hh_count} records in household_type table")
            
            # Verify family_type table
            fam_count = conn.execute(text("SELECT COUNT(*) FROM family_type")).scalar()
            logger.info(f"Found {fam_count} records in family_type table")
            
            # Verify probabilities table
            prob_count = conn.execute(text(
                "SELECT COUNT(*) FROM household_family_type_probabilities"
            )).scalar()
            logger.info(f"Found {prob_count} records in probabilities table")
            
            # Check for data consistency
            if state_count != hh_count or state_count != fam_count or state_count != prob_count:
                logger.error("Data count mismatch across tables")
                return False
            
            # Check for missing values in key fields
            missing_values = conn.execute(text("""
                SELECT COUNT(*) FROM household_family_type_probabilities
                WHERE married_with_children IS NULL
                OR married_no_children IS NULL
                OR single_parent_male IS NULL
                OR single_parent_female IS NULL
                OR single_person IS NULL
            """)).scalar()
            
            if missing_values > 0:
                logger.error(f"Found {missing_values} records with missing values")
                return False
            
            # Basic data quality check
            invalid_probabilities = conn.execute(text("""
                SELECT COUNT(*) FROM household_family_type_probabilities
                WHERE married_with_children < 0 OR married_with_children > 1
                OR married_no_children < 0 OR married_no_children > 1
                OR single_parent_male < 0 OR single_parent_male > 1
                OR single_parent_female < 0 OR single_parent_female > 1
                OR single_person < 0 OR single_person > 1
            """)).scalar()
            
            if invalid_probabilities > 0:
                logger.error(f"Found {invalid_probabilities} records with invalid probabilities")
                return False
            
            # Generate a report
            report = {
                "verification_time": datetime.now().isoformat(),
                "status": "success",
                "metrics": {
                    "state_count": state_count,
                    "household_type_count": hh_count,
                    "family_type_count": fam_count,
                    "probabilities_count": prob_count
                },
                "data_quality": {
                    "missing_values": missing_values,
                    "invalid_probabilities": invalid_probabilities
                }
            }
            
            # Save report as JSON
            with open("reports/data_update_report.json", "w") as f:
                json.dump(report, f, indent=4)
            
            logger.info("Data verification successful")
            return True
            
    except Exception as e:
        logger.error(f"Verification failed: {str(e)}")
        
        # Generate an error report
        report = {
            "verification_time": datetime.now().isoformat(),
            "status": "failure",
            "error": str(e)
        }
        
        # Save report as JSON
        with open("reports/data_update_report.json", "w") as f:
            json.dump(report, f, indent=4)
        
        return False

if __name__ == "__main__":
    verify_data_load()
