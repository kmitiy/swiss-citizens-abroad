CREATE TABLE steering.bfs_publications
(
    record_id INT GENERATED ALWAYS AS IDENTITY (START WITH 100000) PRIMARY KEY,
    load_id integer NOT NULL,
    created_ts timestamp with time zone NOT NULL,
    uuid uuid NOT NULL,
    gnp text NOT NULL,
    dam_id integer NOT NULL,
    title text NOT NULL,
    published_ts timestamp with time zone NOT NULL,
    institution_lvl_0_id integer,
    institution_lvl_1_id integer,
    institution_lvl_0_name text,
    institution_lvl_1_name text,
    prodima_lvl_0_id integer,
    prodima_lvl_1_id integer,
    prodima_lvl_0_code text,
    prodima_lvl_1_code text,
    prodima_lvl_0_name text,
    prodima_lvl_1_name text,
    short_text_gnp text,
    languages text
);

ALTER TABLE IF EXISTS steering.bfs_publications
    OWNER to kmitiy;

GRANT ALL ON TABLE steering.bfs_publications TO kmitiy;