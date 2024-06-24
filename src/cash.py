from src.db_api import Postgresql_DB_API
from src.settings import MINPTS, EPS


class CASH:
    """
    A class that implements the CASH clustering algorithm using a PostgreSQL database.

    Methods:
    --------
    fit(db: Postgresql_DB_API):
        Executes the CASH algorithm on the provided database instance.
    """

    def __init__(self) -> None:
        pass

    def fit(self, db: Postgresql_DB_API):
        """
        Executes the CASH algorithm on the provided database instance.

        Description:
        Defines and executes the CASH algorithm as a PostgreSQL function. This algorithm iterates
        through permutations, calculates deltas, extracts clusters, filters them, deletes clustered
        objects, and cleans the deltas table until the stopping conditions are met.

        Parameters:
        db (Postgresql_DB_API): The database API instance used for database operations.

        Returns:
        None
        """
        cash = """CREATE OR REPLACE FUNCTION cash(eps double precision, minPts int) RETURNS void AS $$
                    DECLARE
                    alphas double precision[];
                    clustered_objects int[];
                    pointer int;
                    p int;
                    BEGIN

                    p := 1;
                    WHILE permutation_left() AND(SELECT COUNT(*) FROM data) >= minPts LOOP
                        pointer := nextval(pg_get_serial_sequence('clusters', 'clusterid'));
                        alphas := get_next_permutation();

                        PERFORM update_next_permutation();
                        PERFORM insert_deltas(alphas);
                        PERFORM extract_clusters(eps, alphas);
                        PERFORM filter_clusters(minPts);
                        PERFORM delete_clustered_objects(pointer);
                        PERFORM clean_deltas_table();
                        p := p + 1;
                    END LOOP;
                    END;
                $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=cash)
        db.execute_engine_query(query=
                """DO $$
                        BEGIN
                        PERFORM cash("""
            + str(EPS)
            + """, """
            + str(MINPTS)
            + """);
                        END;
                        $$;
                    """
            )
