--DROP TABLE tokens;
CREATE TABLE tokens (token varchar(60) primary key, authorized_uses integer);

GRANT SELECT ON tokens TO lambda_client;

--DROP TABLE results;
CREATE TABLE results (result_id varchar(60) primary key, time_inserted timestamp, result_value text);

GRANT INSERT, SELECT ON results TO lambda_client;

CREATE INDEX idx_tokens ON tokens(token);
CREATE INDEX idx_results ON results(result_id);