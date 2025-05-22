-- sql/init.sql
-- Database initialization script for Census Flux
-- This runs automatically when the MySQL container starts or when called by GitHub Actions

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

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_states_name ON states(state_name);
CREATE INDEX IF NOT EXISTS idx_household_updated ON household_type(last_updated);
CREATE INDEX IF NOT EXISTS idx_family_updated ON family_type(last_updated);
CREATE INDEX IF NOT EXISTS idx_probabilities_updated ON household_family_type_probabilities(last_updated);
CREATE INDEX IF NOT EXISTS idx_probabilities_state_name ON household_family_type_probabilities(state_name);

-- Insert a test record to verify setup (will be replaced by real data)
INSERT INTO states (state_code, state_name) VALUES ('00', 'Test State - Setup Verification')
ON DUPLICATE KEY UPDATE state_name = 'Test State - Setup Verification';

-- Display setup completion message
SELECT 'Census Flux database schema initialized successfully!' as status;
SELECT COUNT(*) as tables_created FROM information_schema.tables WHERE table_schema = 'census_flux';

-- Show all tables
SHOW TABLES;