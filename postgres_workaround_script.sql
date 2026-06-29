CREATE TABLE gold.biogrid_interactors(
    "interactor_a" INT NOT NULL,
    "interactors_b" VARCHAR(64) ARRAY NOT NULL,
    CONSTRAINT pk_biogrid_interactors PRIMARY KEY ("interactor_a")
);


CREATE OR REPLACE FUNCTION gold.get_biogrid_interactors()
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
BEGIN

    TRUNCATE TABLE gold.biogrid_interactors;

    INSERT INTO gold.biogrid_interactors
    SELECT
        bd."biogrid_id_interactor_a" as interactor_a,
        ARRAY_AGG(bd."biogrid_id_interactor_b") as interactors_b
    FROM biogrid_data bd
    GROUP BY bd."biogrid_id_interactor_a";

END;
$BODY$;

SELECT gold.get_biogrid_interactors();
