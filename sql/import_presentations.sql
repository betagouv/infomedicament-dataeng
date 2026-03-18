-- Import presentations data from CSV
--
-- The source CSV has an extra leading column (Req_2_Récipient_codeVU) not
-- present in the target table, so we stage through a temp table.
--
-- Usage:
--   Connect to the database interactively and run the commands below,
--   replacing the file path with the absolute path to presentations.csv.
--
--   NOTE: \copy does NOT support psql variable interpolation (:'var' syntax).
--   This is a documented psql limitation, not a bug.
--   See: https://www.postgresql.org/docs/current/app-psql.html
--
-- Example:
--   psql $APP_DB_URL
--   -- Then paste the commands below, replacing paths:

-- Stage CSV (18 columns including the extra leading one)
CREATE TEMP TABLE presentations_staging (
    req_2_recipientcodevu text,
    numpresentation       text,
    codecip13             text,
    nom_presentation      text,
    numelement            text,
    nomelement            text,
    recipient             text,
    numrecipient          text,
    nbrrecipient          text,
    qtecontenance         text,
    codeunitecontenance   text,
    unitecontenance       text,
    codecaraccomplrecip   text,
    numordreedit          text,
    caraccomplrecip       text,
    numdispositif         text,
    codenaturedispositif  text,
    dispositif            text
);

-- Replace the path below with the absolute path to presentations.csv
\copy presentations_staging FROM '/path/to/presentations.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- Empty existing table
TRUNCATE TABLE presentations CASCADE;

-- Load into target table, casting to the right types
INSERT INTO presentations
SELECT
    numpresentation::integer,
    codecip13,
    nom_presentation,
    numelement::integer,
    nomelement,
    recipient,
    numrecipient::integer,
    nbrrecipient::integer,
    qtecontenance::double precision,
    codeunitecontenance,
    unitecontenance,
    codecaraccomplrecip,
    numordreedit::integer,
    caraccomplrecip,
    numdispositif::integer,
    codenaturedispositif,
    dispositif
FROM presentations_staging;

-- Check
SELECT 'presentations' as table_name, COUNT(*) as row_count FROM presentations;
