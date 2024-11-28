#!/bin/bash

# Start PostgreSQL in the background
docker-entrypoint.sh postgres & 

# Wait until PostgreSQL is ready
until psql -U ${DATALAKE_DB_USER} -d postgres -c 'SELECT 1'; do sleep 1; done;

# Append listen_addresses to postgresql.conf
echo "listen_addresses = '*'" >> /var/lib/postgresql/data/postgresql.conf

# Append the connection rule to pg_hba.conf
echo "host    all             all             0.0.0.0/0            md5" >> /var/lib/postgresql/data/pg_hba.conf

# Enable firewall rules for PostgreSQL
sudo ufw allow 5432/tcp
sudo ufw enable

# Create the database and set privileges
psql -U ${DATALAKE_DB_USER} -d postgres -c "CREATE DATABASE ${DATALAKE_DB};" &&
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "GRANT ALL PRIVILEGES ON DATABASE ${DATALAKE_DB} TO ${DATALAKE_DB_USER};" &&
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "ALTER USER ${DATALAKE_DB_USER} WITH SUPERUSER;" &&

# Create the schema for the pipeline
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "CREATE SCHEMA IF NOT EXISTS pipeline;" &&

# Create the table inside the schema
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "
  CREATE TABLE IF NOT EXISTS pipeline.dag_execution_tracking (
      dag_name VARCHAR(255) PRIMARY KEY,
      execution_date TIMESTAMP NOT NULL,
      insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
" &&

# Create the stored procedure to update execution date
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "
  CREATE OR REPLACE FUNCTION pipeline.update_execution_date(dag_name VARCHAR, new_date TIMESTAMP) RETURNS VOID AS $$
  BEGIN
    UPDATE pipeline.dag_execution_tracking
    SET execution_date = new_date, update_date = CURRENT_TIMESTAMP
    WHERE dag_name = dag_name;
    IF NOT FOUND THEN
      INSERT INTO pipeline.dag_execution_tracking (dag_name, execution_date)
      VALUES (dag_name, new_date);
    END IF;
  END;
  $$ LANGUAGE plpgsql;
" &&

# Create function to retrieve execution timestamp
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "
  CREATE OR REPLACE FUNCTION pipeline.get_execution_timestamp(dag_name VARCHAR) RETURNS TIMESTAMP AS $$
  DECLARE
    result_timestamp TIMESTAMP;
  BEGIN
    SELECT execution_date INTO result_timestamp
    FROM pipeline.dag_execution_tracking
    WHERE dag_name = dag_name;
    RETURN result_timestamp;
  END;
  $$ LANGUAGE plpgsql;
" &&

# Create trigger that will update timestamp on any update to the dag_execution_tracking table
psql -U ${DATALAKE_DB_USER} -d ${DATALAKE_DB} -c "
  CREATE TRIGGER update_execution_timestamp
  BEFORE UPDATE ON pipeline.dag_execution_tracking
  FOR EACH ROW
  EXECUTE FUNCTION pipeline.update_execution_date(NEW.dag_name, CURRENT_TIMESTAMP);
" &&

# Restart PostgreSQL to apply changes
pg_ctl restart -D /var/lib/postgresql/data

# Wait for PostgreSQL to continue running
wait