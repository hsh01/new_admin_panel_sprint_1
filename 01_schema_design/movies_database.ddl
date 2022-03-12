CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title varchar(255) NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type varchar(7) NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX IF NOT EXISTS film_work_creation_date_idx ON content.film_work(creation_date);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name varchar(255) NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role varchar(255) NOT NULL,
    created timestamp with time zone
); 

CREATE INDEX IF NOT EXISTS film_work_person_idx ON content.person_film_work (film_work_id, person_id);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name varchar(255) NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX IF NOT EXISTS genre_name_idx ON content.genre(name);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    genre_id uuid NOT NULL,
    created timestamp with time zone
);

CREATE UNIQUE INDEX IF NOT EXISTS genre_film_work_person_idx ON content.genre_film_work (film_work_id, genre_id);