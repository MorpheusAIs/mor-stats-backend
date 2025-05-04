-- Create emissions table
CREATE TABLE IF NOT EXISTS emissions (
    id SERIAL PRIMARY KEY,
    day INTEGER NOT NULL,
    date DATE NOT NULL UNIQUE,
    capital_emission DECIMAL(20, 8) NOT NULL,
    code_emission DECIMAL(20, 8) NOT NULL,
    compute_emission DECIMAL(20, 8) NOT NULL,
    community_emission DECIMAL(20, 8) NOT NULL,
    protection_emission DECIMAL(20, 8) NOT NULL,
    total_emission DECIMAL(20, 8) NOT NULL,
    total_supply DECIMAL(20, 8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index on date for faster lookups
CREATE INDEX IF NOT EXISTS idx_emissions_date ON emissions(date);

-- Create index on day for faster lookups
CREATE INDEX IF NOT EXISTS idx_emissions_day ON emissions(day);

-- Add comments to the table and columns
COMMENT ON TABLE emissions IS 'Stores daily emission data for different categories';
COMMENT ON COLUMN emissions.day IS 'Day number';
COMMENT ON COLUMN emissions.date IS 'Date of emission';
COMMENT ON COLUMN emissions.capital_emission IS 'Capital emission amount';
COMMENT ON COLUMN emissions.code_emission IS 'Code emission amount';
COMMENT ON COLUMN emissions.compute_emission IS 'Compute emission amount';
COMMENT ON COLUMN emissions.community_emission IS 'Community emission amount';
COMMENT ON COLUMN emissions.protection_emission IS 'Protection emission amount';
COMMENT ON COLUMN emissions.total_emission IS 'Total emission amount';
COMMENT ON COLUMN emissions.total_supply IS 'Total supply at this date';