-- =========================================================
-- energyLedger Database Schema
-- Purpose: Database setup with checks for energy and monetary balance laws.
-- Features added: Materialized Views, JSONB metrics, Notifications, Parallelized Loading
-- =========================================================

-- ======================
-- Credits
-- ======================
-- Built on: https://github.com/open-risk/energyLedger/tree/main

-- ====================================
-- Session and Configuration Settings
-- ====================================
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE EXTENSION IF NOT EXISTS plpython3u WITH SCHEMA pg_catalog;

-- ==============================
-- Table and Sequence Definitions
-- ==============================

-- Table: account
CREATE TABLE public.account (
    id integer NOT NULL,
    name text,
    code text,
    symbol text,
    type text NOT NULL -- AS (Asset), LI (Liability), EQ (Equity)
);

ALTER TABLE public.account OWNER TO postgres;
CREATE SEQUENCE public.account_id_seq AS integer START WITH 1 INCREMENT BY 1 CACHE 1;
ALTER SEQUENCE public.account_id_seq OWNED BY public.account.id;
ALTER TABLE ONLY public.account ALTER COLUMN id SET DEFAULT nextval('public.account_id_seq'::regclass);

-- Table: transaction
CREATE TABLE public.transaction (
    id integer NOT NULL,
    type integer NOT NULL,
    "timestamp" timestamp without time zone,
    date date,
    descriptions text
);

ALTER TABLE public.transaction OWNER TO postgres;
CREATE SEQUENCE public.transaction_id_seq AS integer START WITH 1 INCREMENT BY 1 CACHE 1;
ALTER SEQUENCE public.transaction_id_seq OWNED BY public.transaction.id;
ALTER TABLE ONLY public.transaction ALTER COLUMN id SET DEFAULT nextval('public.transaction_id_seq'::regclass);

-- Table: transaction_leg with JSONB metrics for flexible storage
CREATE TABLE public.transaction_leg (
    id integer NOT NULL,
    metrics JSONB,
    account_id integer,
    transaction_id integer,
    description text
);

ALTER TABLE public.transaction_leg OWNER TO postgres;
CREATE SEQUENCE public.transaction_leg_id_seq AS integer START WITH 1 INCREMENT BY 1 CACHE 1;
ALTER SEQUENCE public.transaction_leg_id_seq OWNED BY public.transaction_leg.id;
ALTER TABLE ONLY public.transaction_leg ALTER COLUMN id SET DEFAULT nextval('public.transaction_leg_id_seq'::regclass);

-- ===================================
-- Indexes for Performance Optimization
-- ===================================
CREATE INDEX idx_transaction_leg_transaction_id ON public.transaction_leg (transaction_id);
CREATE INDEX idx_transaction_leg_account_id ON public.transaction_leg (account_id);
CREATE INDEX idx_transaction_type ON public.transaction (type);

-- ==========================================
-- Consolidated Function: check_physical_laws
-- ==========================================
-- Validates transactions against physical and financial laws.
-- Sends NOTIFY alerts if any law is violated.
CREATE FUNCTION public.check_physical_laws() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
                DECLARE
                    monetary_sum DECIMAL(13, 2) := 0;
                    physical_sum DECIMAL(13, 2) := 0;
                    embodied_sum DECIMAL(13, 2) := 0;
                    energy_sum DECIMAL(13, 2) := 0;
                    t_type INTEGER;
                    transaction_id INT;
                BEGIN
                    transaction_id := CASE WHEN TG_OP = 'INSERT' THEN NEW.transaction_id ELSE OLD.transaction_id END;
                    
                    -- Retrieve transaction type to determine if checks are needed
                    SELECT type INTO t_type FROM transaction WHERE id = transaction_id;

                    -- Only perform checks if transaction type is 1
                    IF t_type = 1 THEN
                        SELECT 
                            COALESCE(SUM((metrics->>'monetary_amount')::DECIMAL), 0),
                            COALESCE(SUM((metrics->>'physical_energy')::DECIMAL), 0),
                            COALESCE(SUM((metrics->>'embodied_energy')::DECIMAL), 0),
                            COALESCE(SUM(CASE WHEN account.type = 'AS' THEN 
                                (metrics->>'physical_energy')::DECIMAL + (metrics->>'embodied_energy')::DECIMAL ELSE 0 END), 0)
                        INTO monetary_sum, physical_sum, embodied_sum, energy_sum
                        FROM transaction_leg
                        JOIN account ON transaction_leg.account_id = account.id
                        WHERE transaction_leg.transaction_id = transaction_id;
                    END IF;
                    
                    -- Validations for physical and financial laws
                    IF monetary_sum != 0 THEN
                        PERFORM pg_notify('law_violation', 'Monetary balance violation');
                        RAISE EXCEPTION 'Monetary balance violation: %', monetary_sum;
                    END IF;
                    IF physical_sum != 0 THEN
                        PERFORM pg_notify('law_violation', 'Physical energy balance violation');
                        RAISE EXCEPTION 'Physical energy balance violation: %', physical_sum;
                    END IF;
                    IF embodied_sum != 0 THEN
                        PERFORM pg_notify('law_violation', 'Embodied energy balance violation');
                        RAISE EXCEPTION 'Embodied energy balance violation: %', embodied_sum;
                    END IF;
                    IF energy_sum != 0 THEN
                        PERFORM pg_notify('law_violation', 'Energy conservation violation');
                        RAISE EXCEPTION 'Energy conservation violation: %', energy_sum;
                    END IF;
                    IF embodied_sum < 0 THEN
                        PERFORM pg_notify('law_violation', 'Entropy violation');
                        RAISE EXCEPTION 'Entropy violation: embodied energy decreased by %', embodied_sum;
                    END IF;
                    
                    RETURN NEW;
                END;
    $$;

-- =============================================
-- Unified Trigger for Transaction Leg Validation
-- =============================================
DROP TRIGGER IF EXISTS balance_trigger ON public.transaction_leg;
DROP TRIGGER IF EXISTS energy_trigger ON public.transaction_leg;
DROP TRIGGER IF EXISTS entropy_trigger ON public.transaction_leg;

CREATE CONSTRAINT TRIGGER physical_laws_trigger 
AFTER INSERT OR DELETE OR UPDATE ON public.transaction_leg
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.check_physical_laws();

-- ===========================================
-- Materialized View for Summarized Balances
-- ===========================================
-- Stores precomputed balances by transaction ID to reduce recalculations.
CREATE MATERIALIZED VIEW transaction_balances AS
SELECT 
    transaction_id, 
    SUM((metrics->>'monetary_amount')::DECIMAL) AS monetary_balance,
    SUM((metrics->>'physical_energy')::DECIMAL) AS physical_balance,
    SUM((metrics->>'embodied_energy')::DECIMAL) AS embodied_balance
FROM transaction_leg
GROUP BY transaction_id;

-- Refresh strategy: REFRESH MATERIALIZED VIEW transaction_balances;

-- ===========================================
-- Initial Data Insertion for Tables
-- ===========================================
-- Account data
COPY public.account (id, name, code, symbol, type) FROM stdin;
1	Cash	A01	C	AS
2	Factory	A02	F	AS
3	Energy Stock	A03	S	AS
4	Raw Materials	A04	M	AS
5	Inventory	A05	I	AS
6	Accounts Payable	A06	P	LI
7	Bank Loan	A07	L	LI
8	Equity	A08	K	EQ
\.

-- Transaction data
COPY public.transaction (id, type, "timestamp", date, descriptions) FROM stdin;
1	0	2023-05-17 21:23:52.470828	2023-01-02	Initial Equity Transaction
-- Additional transactions follow as required
\.

-- Parallel Data Loading for transaction_leg
COPY public.transaction_leg (id, metrics, account_id, transaction_id, description) 
FROM '/path/to/transaction_leg_data.csv' WITH (FORMAT csv, DELIMITER ',', HEADER, PARALLEL 8);

-- ===============================
-- Sequence Values Initialization
-- ===============================
SELECT pg_catalog.setval('public.account_id_seq', 8, true);
SELECT pg_catalog.setval('public.transaction_id_seq', 1, false);
SELECT pg_catalog.setval('public.transaction_leg_id_seq', 7, true);

-- ======================
-- Constraints Definition
-- ======================
ALTER TABLE ONLY public.account ADD CONSTRAINT account_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.transaction ADD CONSTRAINT transaction_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.transaction_leg ADD CONSTRAINT transaction_leg_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.transaction_leg
    ADD CONSTRAINT fk_account FOREIGN KEY (account_id) REFERENCES public.account(id);
ALTER TABLE ONLY public.transaction_leg
    ADD CONSTRAINT fk_transaction FOREIGN KEY (transaction_id) REFERENCES public.transaction(id);
