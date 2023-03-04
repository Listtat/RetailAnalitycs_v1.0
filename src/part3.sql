CREATE ROLE visitor WITH LOGIN PASSWORD 'boba';
GRANT CONNECT ON DATABASE postgres TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to visitor;


CREATE ROLE admin WITH LOGIN PASSWORD 'biba'
SUPERUSER CREATEDB CREATEROLE;


---drop role reader
REASSIGN OWNED BY visitor TO postgres;
DROP OWNED BY visitor;
drop role visitor;

---drop role admin
REASSIGN OWNED BY admin TO postgres;
DROP OWNED BY admin;
DROP ROLE admin;