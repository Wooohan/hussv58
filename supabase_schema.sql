-- ============================================================================
-- TABLE 1: CARRIERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS carriers (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    mc_number TEXT NOT NULL UNIQUE,
    dot_number TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    dba_name TEXT,
    entity_type TEXT,
    status TEXT,
    email TEXT,
    phone TEXT,
    power_units TEXT,
    drivers TEXT,
    non_cmv_units TEXT,
    physical_address TEXT,
    mailing_address TEXT,
    date_scraped TEXT,
    mcs150_date TEXT,
    mcs150_mileage TEXT,
    operation_classification TEXT[],
    carrier_operation TEXT[],
    cargo_carried TEXT[],
    out_of_service_date TEXT,
    state_carrier_id TEXT,
    duns_number TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    basic_scores JSONB,
    oos_rates JSONB,
    insurance_policies JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for carriers table
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_created_at ON carriers(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_carriers_status ON carriers(status);

-- Enable RLS for carriers
ALTER TABLE carriers ENABLE ROW LEVEL SECURITY;

-- RLS Policies for carriers table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON carriers;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON carriers;

CREATE POLICY "Enable read access for anonymous users" ON carriers
    FOR SELECT
    USING (true);

CREATE POLICY "Enable all access for authenticated users" ON carriers
    FOR ALL
    USING (true)
    WITH CHECK (true);

ALTER TABLE carriers ADD COLUMN IF NOT EXISTS crashes JSONB;
ALTER TABLE carriers ADD COLUMN IF NOT EXISTS inspections JSONB;

-- ============================================================================
-- TABLE 2: FMCSA_REGISTER (MISSING IN ORIGINAL SCHEMA - NOW ADDED)
-- ============================================================================
CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number TEXT NOT NULL,
    title TEXT NOT NULL,
    decided TEXT,
    category TEXT,
    date_fetched TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);

-- Create indexes for fmcsa_register table
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

-- Enable RLS for fmcsa_register
ALTER TABLE fmcsa_register ENABLE ROW LEVEL SECURITY;

-- RLS Policies for fmcsa_register table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON fmcsa_register;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON fmcsa_register;

CREATE POLICY "Enable read access for anonymous users" ON fmcsa_register
    FOR SELECT
    USING (true);

CREATE POLICY "Enable all access for authenticated users" ON fmcsa_register
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- ============================================================================
-- TABLE 3: USERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    plan TEXT NOT NULL DEFAULT 'Free' CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active TEXT DEFAULT 'Never',
    ip_address TEXT,
    is_online BOOLEAN DEFAULT false,
    is_blocked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for users table
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

-- Enable RLS for users
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

-- RLS Policies for users table
DROP POLICY IF EXISTS "Enable read access for anonymous users" ON users;
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON users;

CREATE POLICY "Enable read access for anonymous users" ON users
    FOR SELECT
    USING (true);

CREATE POLICY "Enable all access for authenticated users" ON users
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- ============================================================================
-- TABLE 4: BLOCKED_IPS
-- ============================================================================
CREATE TABLE IF NOT EXISTS blocked_ips (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason TEXT,
    blocked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    blocked_by TEXT
);

-- Create indexes for blocked_ips table
CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

-- Enable RLS for blocked_ips
ALTER TABLE blocked_ips ENABLE ROW LEVEL SECURITY;

-- RLS Policies for blocked_ips table
DROP POLICY IF EXISTS "Enable all access for blocked_ips" ON blocked_ips;
DROP POLICY IF EXISTS "Enable read access for blocked_ips" ON blocked_ips;

CREATE POLICY "Enable all access for blocked_ips" ON blocked_ips
    FOR ALL
    USING (true)
    WITH CHECK (true);

CREATE POLICY "Enable read access for blocked_ips" ON blocked_ips
    FOR SELECT
    USING (true);

-- ============================================================================
-- TRIGGER FUNCTIONS FOR AUTO-UPDATING TIMESTAMPS
-- ============================================================================

-- Function for carriers table
CREATE OR REPLACE FUNCTION update_carriers_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for carriers table
DROP TRIGGER IF EXISTS update_carriers_updated_at ON carriers;
CREATE TRIGGER update_carriers_updated_at BEFORE UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION update_carriers_updated_at();

-- Function for fmcsa_register table
CREATE OR REPLACE FUNCTION update_fmcsa_register_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for fmcsa_register table
DROP TRIGGER IF EXISTS update_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER update_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_fmcsa_register_updated_at();

-- Function for users table
CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for users table
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert default admin user (only if not exists)
INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;

-- ============================================================================
-- DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE carriers IS 'FMCSA carrier data with insurance and safety information';
COMMENT ON COLUMN carriers.mc_number IS 'MC/MX Number - Unique identifier';
COMMENT ON COLUMN carriers.dot_number IS 'USDOT Number';
COMMENT ON COLUMN carriers.insurance_policies IS 'JSON array of insurance policies';
COMMENT ON COLUMN carriers.basic_scores IS 'JSON array of BASIC performance scores';
COMMENT ON COLUMN carriers.oos_rates IS 'JSON array of Out-of-Service rates';

COMMENT ON TABLE fmcsa_register IS 'FMCSA Daily Register entries with motor carrier decisions and notices';
COMMENT ON COLUMN fmcsa_register.number IS 'Docket number (e.g., MC-123456)';
COMMENT ON COLUMN fmcsa_register.title IS 'Entry title or description';
COMMENT ON COLUMN fmcsa_register.decided IS 'Date decided (MM/DD/YYYY format)';
COMMENT ON COLUMN fmcsa_register.category IS 'Category of decision (NAME CHANGE, REVOCATION, etc.)';
COMMENT ON COLUMN fmcsa_register.date_fetched IS 'Date when this entry was scraped';

COMMENT ON TABLE users IS 'User accounts for FreightIntel AI application';
COMMENT ON TABLE blocked_ips IS 'Blocked IP addresses for security';
COMMENT ON COLUMN users.user_id IS 'Application-level unique user ID';
COMMENT ON COLUMN users.role IS 'User role: user or admin';
COMMENT ON COLUMN users.plan IS 'Subscription plan: Free, Starter, Pro, Enterprise';
COMMENT ON COLUMN users.daily_limit IS 'Maximum MC records allowed per day';
COMMENT ON COLUMN users.is_blocked IS 'Whether the user is blocked from accessing the system';
