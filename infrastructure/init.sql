-- sql/init.sql
-- Database initialization script for Census Flux
-- This script is designed to be idempotent (safe to run multiple times)

-- Ensure we're using the correct database
USE census_flux;

-- Create states table
CREATE TABLE IF NOT EXISTS states (
    state_code VARCHAR(10) PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create household_type table
CREATE TABLE IF NOT EXISTS household_type (
    state_code VARCHAR(10) PRIMARY KEY,
    total_households INT NOT NULL,
    married_couple INT NOT NULL,
    male_householder INT NOT NULL,
    female_householder INT NOT NULL,
    living_alone INT NOT NULL,
    not_living_alone INT NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (state_code) REFERENCES states(state_code) ON DELETE CASCADE
);

-- Create family_type table
CREATE TABLE IF NOT EXISTS family_type (
    state_code VARCHAR(10) PRIMARY KEY,
    married_with_children INT NOT NULL,
    male_with_children INT NOT NULL,
    female_with_children INT NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (state_code) REFERENCES states(state_code) ON DELETE CASCADE
);

-- Create household_family_type_probabilities table
CREATE TABLE IF NOT EXISTS household_family_type_probabilities (
    state_code VARCHAR(10) PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    married_with_children DECIMAL(10,8) NOT NULL,
    married_no_children DECIMAL(10,8) NOT NULL,
    single_parent_male DECIMAL(10,8) NOT NULL,
    single_parent_female DECIMAL(10,8) NOT NULL,
    single_person DECIMAL(10,8) NOT NULL,
    other_nonfamily DECIMAL(10,8) NOT NULL,
    other_family_male DECIMAL(10,8) NOT NULL,
    other_family_female DECIMAL(10,8) NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (state_code) REFERENCES states(state_code) ON DELETE CASCADE
);

-- Create indexes with error handling
-- Note: MySQL 8.0+ supports DROP INDEX IF EXISTS, older versions need this approach

-- Helper procedure to create indexes safely
DELIMITER $$

CREATE PROCEDURE CreateIndexIfNotExists(
    IN table_name VARCHAR(64),
    IN index_name VARCHAR(64),
    IN index_definition TEXT
)
BEGIN
    DECLARE index_exists INT DEFAULT 0;
    
    -- Check if index exists
    SELECT COUNT(*) INTO index_exists
    FROM information_schema.statistics 
    WHERE table_schema = DATABASE() 
    AND table_name = table_name 
    AND index_name = index_name;
    
    -- Create index if it doesn't exist
    IF index_exists = 0 THEN
        SET @sql = CONCAT('CREATE INDEX ', index_name, ' ON ', table_name, ' ', index_definition);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$

DELIMITER ;

-- Create indexes using the safe procedure
CALL CreateIndexIfNotExists('states', 'idx_states_name', '(state_name)');
CALL CreateIndexIfNotExists('household_type', 'idx_household_updated', '(last_updated)');
CALL CreateIndexIfNotExists('family_type', 'idx_family_updated', '(last_updated)');
CALL CreateIndexIfNotExists('household_family_type_probabilities', 'idx_probabilities_updated', '(last_updated)');
CALL CreateIndexIfNotExists('household_family_type_probabilities', 'idx_probabilities_state_name', '(state_name)');

-- Drop the helper procedure
DROP PROCEDURE IF EXISTS CreateIndexIfNotExists;

-- Insert a test record to verify setup (will be replaced by real data)
INSERT INTO states (state_code, state_name) VALUES ('00', 'Test State - Setup Verification')
ON DUPLICATE KEY UPDATE 
    state_name = 'Test State - Setup Verification',
    last_updated = CURRENT_TIMESTAMP;

-- Display setup completion message
SELECT 'Census Flux database schema initialized successfully!' as status;
SELECT COUNT(*) as tables_created 
FROM information_schema.tables 
WHERE table_schema = DATABASE() 
AND table_type = 'BASE TABLE';

-- Show all tables
SHOW TABLES;

-- Show indexes
SELECT 
    table_name,
    index_name,
    column_name
FROM information_schema.statistics 
WHERE table_schema = DATABASE() 
ORDER BY table_name, index_name, seq_in_index;