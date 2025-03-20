CREATE TABLE public.bfs_publications
(
    record_id integer NOT NULL,
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
    languages text,
    PRIMARY KEY (record_id)
);

ALTER TABLE IF EXISTS public.bfs_publications
    OWNER to postgres;

GRANT ALL ON TABLE public.bfs_publications TO kmitiy;