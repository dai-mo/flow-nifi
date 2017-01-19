CREATE TABLE IF NOT EXISTS dcs.flow_data_content (
  id varchar PRIMARY KEY,
  claim_count int,
  timestamp timestamp,
  data blob
);

CREATE TABLE IF NOT EXISTS dcs.flow_data_provenance (
  id varchar PRIMARY KEY,
  event_id double,
  event_time double,
  flow_file_entry_date double,
  lineage_start_entry_date double,
  file_size double,
  previous_file_size double,
  event_duration double,
  event_type varchar,
  attributes varchar,
  previous_attributes varchar,
  updated_attributes varchar,
  component_id varchar,
  component_type varchar,
  transit_uri varchar,
  source_system_flow_file_identifier varchar,
  flow_file_uuid varchar,
  parent_uuids varchar,
  child_uuids varchar,
  alternate_identifier_uri varchar,
  details varchar,
  relationship varchar,
  source_queue_identifier varchar,
  content_claim_identifier varchar,
  previous_content_claim_identifier varchar
);

CREATE INDEX IF NOT EXISTS event_id ON dcs.flow_data_provenance (event_id);
CREATE INDEX IF NOT EXISTS event_type ON dcs.flow_data_provenance (event_type);
CREATE INDEX IF NOT EXISTS component_id ON dcs.flow_data_provenance (component_id);
CREATE INDEX IF NOT EXISTS flow_file_uuid ON dcs.flow_data_provenance (flow_file_uuid);
CREATE INDEX IF NOT EXISTS relationship ON dcs.flow_data_provenance (relationship);

CREATE TABLE IF NOT EXISTS dcs.flow_id (
	name varchar PRIMARY KEY,
	latest_id counter
);

INSERT INTO dcs.flow_id(name, latest_id) VALUES('provenance_event_id', 0)
