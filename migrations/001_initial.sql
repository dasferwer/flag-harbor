CREATE TABLE users(id uuid PRIMARY KEY,email text UNIQUE NOT NULL,password_hash text NOT NULL,
    role text NOT NULL DEFAULT 'user' CHECK(role IN ('user','admin')));
CREATE TABLE environments(id uuid PRIMARY KEY,owner_id uuid NOT NULL REFERENCES users(id),name text NOT NULL,
    revision bigint NOT NULL DEFAULT 1,enabled boolean NOT NULL DEFAULT true,sdk_key_hash text UNIQUE NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE flags(environment_id uuid REFERENCES environments(id),key text NOT NULL,definition jsonb NOT NULL,
    PRIMARY KEY(environment_id,key));
CREATE TABLE audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment_id uuid NOT NULL REFERENCES environments(id),
    revision bigint NOT NULL,actor_id uuid NOT NULL REFERENCES users(id),action text NOT NULL,flag_key text,
    previous jsonb,next jsonb,created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(environment_id,revision));
