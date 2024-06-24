from src.db_api import Postgresql_DB_API
from src.settings import DATASET_PATH, SPLITS
from datetime import datetime
import pandas as pd
import logging
import os

def start_logging(log_file_path: str = "logs/log.txt") -> None:
    """
    Initializes logging configuration.

    Parameters:
        log_file_path (str): The path to the log file.

    Returns:
        None
    """
    logs_dir = os.path.dirname(log_file_path)
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    logging.basicConfig(filename=log_file_path, filemode="a", level=logging.ERROR)


def create_table_from_csv(db: Postgresql_DB_API, table_name: str):
    """
    Creates a table in the database from a CSV file.

    Description:
    Reads data from a CSV file located at DATASET_PATH and creates a table with the specified
    table_name in the database. Adds a primary key column 'oid' to the table.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    table_name (str): The name of the table to be created in the database.

    Returns:
    None
    """
    try:
        data = pd.read_csv(DATASET_PATH, sep=",", encoding='utf-8')
        data.to_sql(table_name, db.engine, if_exists="replace", index=False)
        db.execute_engine_query(f"ALTER TABLE {table_name} ADD COLUMN oid SERIAL PRIMARY KEY;")
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating table '{table_name}' from CSV: {e}")


def init_helper_functions(db: Postgresql_DB_API):
    """
    Initializes helper functions and extensions in the database.

    Description:
    Creates necessary extensions and calculation functions in the database.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        create_extensions(db=db)
        create_calculation_functions(db=db)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed initializing helper functions: {e}")


def create_extensions(db: Postgresql_DB_API):
    """
    Creates necessary PostgreSQL extensions.

    Description:
    Creates the 'intarray' extension in the database.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        db.execute_engine_query(query="CREATE EXTENSION intarray;")
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating 'intarray' extension: {e}")


def create_calculation_functions(db: Postgresql_DB_API):
    """
    Creates necessary calculation functions in the database.

    Description:
    Creates various calculation functions needed for data processing.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        create_euclidian_dist_function(db=db)
        create_parameterization_function(db=db)
        create_dimensions_function(db=db)
        create_get_object_function(db=db)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating calculation functions: {e}")


def create_euclidian_dist_function(db: Postgresql_DB_API):
    """
    Creates the Euclidean distance calculation function.

    Description:
    Creates a PostgreSQL function to calculate the Euclidean distance between two points.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        euclidian_dist_func = """CREATE OR REPLACE FUNCTION euclidiandist(n1 double precision, n2 double precision) RETURNS double precision AS $$
                                 DECLARE 
                                   result double precision;
                                 BEGIN 
                                   result := |/ ((n1 - n2) ^ 2);
                                   RETURN result;
                                 END;
                                 $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=euclidian_dist_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating Euclidean distance function: {e}")


def create_parameterization_function(db: Postgresql_DB_API):
    """
    Creates the parameterization calculation function.

    Description:
    Creates a PostgreSQL function to perform parameterization calculations on input arrays.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        paramfunc = """CREATE OR REPLACE FUNCTION parameterizationfunc(p double precision[], alphas double precision[]) RETURNS double precision AS $$
                       DECLARE 
                        delta double precision;
                        subsum double precision; 
                        d int; 
                       BEGIN
                        delta := 0;
                        subsum := 0; 
                        d := get_dimension();

                        alphas := alphas || 0;

                        FOR i IN 1..d LOOP
                          subsum := p[i];
                          FOR j IN 1..(i-1) LOOP
                            subsum := subsum * sind(alphas[j]);
                          END LOOP;
                          subsum := subsum * cosd(alphas[i]);
                          delta := delta + subsum; 
                        END LOOP;

                        RETURN delta;
                       END;
                       $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=paramfunc)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating parameterization function: {e}")


def create_dimensions_function(db: Postgresql_DB_API):
    """
    Creates the get_dimension function.

    Description:
    Creates a PostgreSQL function to get the number of dimensions (columns) in the 'data' table.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        query = """CREATE OR REPLACE FUNCTION get_dimension() RETURNS int AS $$
                    BEGIN
                      RETURN(SELECT COUNT(*) - 1 AS num_of_cols
                             FROM information_schema.columns 
                             WHERE table_name = 'data');
                    END; 
                    $$ LANGUAGE PLPGSQL;
                """
        db.execute_engine_query(query=query)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating get_dimension function: {e}")


def create_get_object_function(db: Postgresql_DB_API):
    """
    Creates the get_object function.

    Description:
    Creates a PostgreSQL function to retrieve an array of attribute values for a given object ID.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        query = """ CREATE OR REPLACE FUNCTION get_object(id int) RETURNS double precision[] AS $$
                        DECLARE
                            attr text[];
                            q text;
                            res double precision[];
                        BEGIN
                            q := 'SELECT ARRAY[';
                            
                            attr := ARRAY(SELECT column_name FROM information_schema.columns WHERE table_name = 'data' AND column_name != 'oid');
                            q := q || array_to_string(attr, ',', '*') || '] FROM data WHERE oid = ' || id;
                            EXECUTE q INTO res;
                            RETURN res;
                        END
                    $$ LANGUAGE PLPGSQL
        """
        db.execute_engine_query(query=query)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating get_object function: {e}")


def create_tables(db: Postgresql_DB_API):
    """
    Creates necessary tables in the database.

    Description:
    Creates the tables 'alphas', 'deltas', 'clusters', and 'next_permutation' in the database.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        create_alphas_table(db=db)
        create_deltas_table(db=db)
        create_clusters_table(db=db)
        create_next_permutation_table(db=db)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating tables: {e}")


def create_alphas_table(db: Postgresql_DB_API):
    """
    Creates the 'alphas' table.

    Description:
    Creates the 'alphas' table with columns 'aid' and 'deg'.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        alphas_table = """CREATE TABLE alphas (
                          aid int PRIMARY KEY, 
                          deg double precision
                         );
        """
        db.execute_engine_query(query=alphas_table)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating 'alphas' table: {e}")


def create_deltas_table(db: Postgresql_DB_API):
    """
    Creates the 'deltas' table.

    Description:
    Creates the 'deltas' table with columns 'oid' and 'delta', where 'oid' references the 'data' table.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        deltas_table = """CREATE TABLE deltas (
                          oid int,
                          delta double precision,
                          FOREIGN KEY(oid) REFERENCES data(oid)
                         );
        """
        db.execute_engine_query(query=deltas_table)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating 'deltas' table: {e}")


def create_clusters_table(db: Postgresql_DB_API):
    """
    Creates the 'clusters' table using a PL/pgSQL function.

    Description:
    Creates a PostgreSQL function to dynamically create the 'clusters' table based on the number of dimensions in the 'data' table.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        clusters_table = """CREATE OR REPLACE FUNCTION create_clusters_table() RETURNS void AS $$
                          DECLARE
                            c_table text;
                            alphas int;
                          BEGIN
                            c_table := 'CREATE TABLE clusters(clusterid serial PRIMARY KEY, cluster int[], ';
                            alphas = (SELECT get_dimension()) - 1;
                                FOR alpha in 0..(alphas-1) LOOP
                                    c_table := c_table || 'deg' || alpha || ' double precision, ';
                                END LOOP;
                                c_table := left(c_table, -2);
                                c_table := c_table || ');';
                                EXECUTE c_table;
                          END;
                        $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=clusters_table)
        db.execute_engine_query(query =  """DO $$
                                            BEGIN
                                            PERFORM create_clusters_table();
                                            END;
                                            $$;
                                        """)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating 'clusters' table: {e}")


def create_next_permutation_table(db: Postgresql_DB_API):
    """
    Creates the 'next_permutation' table.

    Description:
    Creates the 'next_permutation' table with columns 'rid' and 'rowpointer'.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        next_permutation_table ="""CREATE TABLE next_permutation(
                                rid int PRIMARY KEY,
                                rowpointer int 
                                );
                                """
        db.execute_engine_query(query=next_permutation_table)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating 'next_permutation' table: {e}")


def create_cash_functions(db: Postgresql_DB_API):
    """
    Creates all necessary functions for the CASH algorithm.

    Description:
    Calls individual functions to create various components needed for the CASH algorithm, including functions for permutations,
    clusters, and data management.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        create_get_next_permutation_function(db=db)
        create_permutation_left_function(db=db)
        create_update_next_permutation_function(db=db)
        create_insert_cluster_function(db=db)
        create_delete_deltas_by_oid_function(db=db)
        create_insert_deltas_function(db=db)
        create_extract_cluster_function(db=db)
        create_filter_clusters_function(db=db)
        create_delete_clustered_objects_function(db=db)
        create_clean_deltas_table_function(db=db)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating CASH functions: {e}")


def create_get_next_permutation_function(db: Postgresql_DB_API):
    """
    Creates the get_next_permutation function.

    Description:
    Creates a PostgreSQL function to get the next permutation of alphas.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        next_permutation_func = """CREATE OR REPLACE FUNCTION get_next_permutation() RETURNS double precision[] AS $$
                                    DECLARE
                                        rows_to_select int[];
                                        alphas double precision[];
                                    BEGIN
                                        rows_to_select := (SELECT array_agg(rowpointer) FROM next_permutation WHERE rid != 0); 
                                        FOR r in 1..array_length(rows_to_select, 1) LOOP
                                            alphas := array_append(alphas, (SELECT deg FROM alphas WHERE aid = rows_to_select[r]));
                                        END LOOP;
                                        
                                        RETURN alphas;
                                    END;
                                $$ LANGUAGE PLPGSQL   
        """
        db.execute_engine_query(query=next_permutation_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating get_next_permutation function: {e}")


def create_permutation_left_function(db: Postgresql_DB_API):
    """
    Creates the permutation_left function.

    Description:
    Creates a PostgreSQL function to check if there are permutations left.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        permutation_left_func = """CREATE OR REPLACE FUNCTION permutation_left() RETURNS boolean AS $$ 
                                    DECLARE 
                                        val int;
                                        maxval int;
                                    BEGIN
                                        maxval := (SELECT MAX(aid) FROM alphas);
                                        val := (SELECT rowpointer FROM next_permutation WHERE rid = 1);
                                        
                                        IF val > maxval THEN 
                                            RETURN false;
                                        END IF;
                                        RETURN true;
                                    END; 
                                $$ LANGUAGE PLPGSQL;
                            """
        db.execute_engine_query(query=permutation_left_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating permutation_left function: {e}")


def create_update_next_permutation_function(db: Postgresql_DB_API):
    """
    Creates the update_next_permutation function.

    Description:
    Creates a PostgreSQL function to update the next permutation of alphas.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        update_func = """CREATE OR REPLACE FUNCTION update_next_permutation() RETURNS void AS $$
                            DECLARE
                                val int;
                                pointer int;
                                maxrow int;
                                maxval int;
                            BEGIN
                                maxrow := (SELECT MAX(rid) FROM next_permutation);
                                pointer := (SELECT rowpointer FROM next_permutation WHERE rid = 0);
                                val := (SELECT rowpointer FROM next_permutation WHERE rid = pointer);
                                maxval := (SELECT MAX(aid) FROM alphas);
                                
                                WHILE val >= maxval LOOP
                                    IF pointer = 1 THEN 
                                    UPDATE next_permutation SET rowpointer = maxval + 1 WHERE rid = pointer;
                                    RETURN;
                                    END IF;
                                
                                    UPDATE next_permutation SET rowpointer = 1 WHERE rid = pointer;
                                    pointer := pointer - 1;
                                    val := (SELECT rowpointer FROM next_permutation WHERE rid = pointer);
                                END LOOP;
                                
                                UPDATE next_permutation SET rowpointer = val + 1 WHERE rid = pointer;
                                IF pointer < maxrow THEN
                                    pointer := pointer + 1;
                                END IF;
                            END;
                        $$ LANGUAGE PLPGSQL;
                    """
        db.execute_engine_query(query=update_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating update_next_permutation function: {e}")


def create_insert_cluster_function(db: Postgresql_DB_API):
    """
    Creates the insert_cluster function.

    Description:
    Creates a PostgreSQL function to insert a cluster into the 'clusters' table.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    cl (int[]): An array of cluster IDs.
    alphas (double precision[]): An array of alpha values.

    Returns:
    None
    """
    try:
        insert_cluster_function = """CREATE OR REPLACE FUNCTION insert_cluster(cl int[], alphas double precision[]) RETURNS void AS $$
                                    BEGIN
                                        EXECUTE ('INSERT INTO clusters
                                                VALUES(nextval(pg_get_serial_sequence(''clusters'', ''clusterid'')), 
                                                sort(ARRAY[' || array_to_string(cl, ',', '*') || ']), ' 
                                                || array_to_string(alphas, ',', '*') || ');');
                                    END;
                               $$ LANGUAGE PLPGSQL;    
        """
        db.execute_engine_query(query=insert_cluster_function)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating insert_cluster function: {e}")


def create_delete_deltas_by_oid_function(db: Postgresql_DB_API):
    """
    Creates the delete_deltas_by_oids function.

    Description:
    Creates a PostgreSQL function to delete delta entries by their object IDs.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    oid_arr (int[]): An array of object IDs.

    Returns:
    None
    """
    try:
        delete_function = """CREATE OR REPLACE FUNCTION delete_deltas_by_oids(oid_arr int[]) RETURNS void AS $$
                            BEGIN
                                FOR o in 1..array_length(oid_arr, 1) LOOP
                                    DELETE FROM deltas WHERE deltas.oid = oid_arr[o];
                                END LOOP;
                            END;
                        $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=delete_function)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating delete_deltas_by_oids function: {e}")


def create_insert_deltas_function(db: Postgresql_DB_API):
    """
    Creates the insert_deltas function.

    Description:
    Creates a PostgreSQL function to insert an entry into the 'deltas' table based on alpha values.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    alphas (double precision[]): An array of alpha values.

    Returns:
    None
    """
    try:
        insert_deltas_function = """CREATE OR REPLACE FUNCTION insert_deltas(alphas double precision[]) RETURNS void AS $$
                                BEGIN 
                                
                                    INSERT INTO deltas
                                    SELECT oid, parameterizationfunc(get_object(oid), alphas)
                                    FROM data;
                                
                                END
                              $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=insert_deltas_function)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating insert_deltas function: {e}")


def create_extract_cluster_function(db: Postgresql_DB_API):
    """
    Creates the extract_clusters function.

    Description:
    Creates a PostgreSQL function to extract clusters based on epsilon and alpha values.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    eps (double precision): The epsilon value for clustering.
    alphas (double precision[]): An array of alpha values.

    Returns:
    None
    """
    try:
        extract_func = """CREATE OR REPLACE FUNCTION extract_clusters(eps double precision, alphas double precision[]) RETURNS void AS $$
                            DECLARE
                                q text;
                                next_cluster int[];
                                cluster_id int;
                            BEGIN
                                q :=  'SELECT sort(array_agg(d2.oid) || d1.oid) 
                                       FROM deltas AS d1, deltas AS d2
                                       WHERE d1.oid != d2.oid AND euclidiandist(d1.delta, d2.delta) <= ' || eps || '  
                                       GROUP BY d1.oid
                                       ORDER BY array_length(array_agg(d2.oid) || d1.oid, 1) DESC
                                       LIMIT 1';
                                EXECUTE q INTO next_cluster;
                                
                                WHILE next_cluster IS NOT NULL LOOP
                                    PERFORM insert_cluster(next_cluster, alphas);
                                    PERFORM delete_deltas_by_oids(next_cluster);
                                    EXECUTE q INTO next_cluster;
                                END LOOP;
                            END;
                        $$ LANGUAGE PLPGSQL;    
        """
        db.execute_engine_query(query=extract_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating extract_clusters function: {e}")


def create_filter_clusters_function(db: Postgresql_DB_API):
    """
    Creates the filter_clusters function.

    Description:
    Creates a PostgreSQL function to filter clusters by a minimum number of points.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    minPts (int): The minimum number of points required for a cluster.

    Returns:
    None
    """
    try:
        filter_func = """CREATE OR REPLACE FUNCTION filter_clusters(minPts int) RETURNS void AS $$
                        BEGIN
                            DELETE 
                            FROM clusters
                            WHERE array_length(cluster, 1) < minPts;  
                        END;
                        $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=filter_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating filter_clusters function: {e}")



def create_delete_clustered_objects_function(db: Postgresql_DB_API):
    """
    Creates the delete_clustered_objects function.

    Description:
    Creates a PostgreSQL function to delete clustered objects from the 'data' and 'deltas' tables.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.
    startid (int): The starting cluster ID from which to delete objects.

    Returns:
    None
    """
    try:
        delete_clustered_objects_func = """CREATE OR REPLACE FUNCTION delete_clustered_objects(startid int) RETURNS void AS $$
                                            DECLARE
                                                cl clusters;
                                                objects_to_delete int[];
                                            BEGIN
                                                IF (SELECT COUNT(*) FROM clusters WHERE clusterid >= startid) = 0 THEN
                                                    RETURN;
                                                END IF;
                                                FOR cl in (SELECT * FROM clusters WHERE clusterid >= startid) LOOP
                                                    objects_to_delete  := objects_to_delete || cl.cluster; 
                                                END LOOP;
                                                IF objects_to_delete IS NULL THEN 
                                                    RETURN;
                                                END IF;
                                                objects_to_delete := uniq(objects_to_delete);                                            
                                                FOR o in 1..array_length(objects_to_delete, 1) LOOP
                                                    DELETE FROM deltas WHERE oid = objects_to_delete[o];
                                                    DELETE FROM data WHERE oid = objects_to_delete[o]; 
                                                END LOOP;
                                            END;
                                        $$ LANGUAGE PLPGSQL;  
        """
        db.execute_engine_query(query=delete_clustered_objects_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating delete_clustered_objects function: {e}")


def create_clean_deltas_table_function(db: Postgresql_DB_API):
    """
    Creates the clean_deltas_table function.

    Description:
    Creates a PostgreSQL function to clean the 'deltas' table by deleting all entries.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        clean_deltas_func = """CREATE OR REPLACE FUNCTION clean_deltas_table() RETURNS void AS $$
                                BEGIN
                                
                                    DELETE FROM deltas;
                                
                                END;
                            $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=clean_deltas_func)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed creating clean_deltas_table function: {e}")


def init_tables(db: Postgresql_DB_API):
    """
    Initializes the necessary tables and inserts initial data.

    Description:
    Calls functions to insert initial alpha values and the first permutation into the respective tables.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        insert_alphas(db=db)
        insert_first_permutation(db=db)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed initializing tables: {e}")


def insert_alphas(db: Postgresql_DB_API):
    """
    Creates and executes the insert_alphas function.

    Description:
    Creates a PostgreSQL function to insert alpha values into the 'alphas' table. The values are generated based on the provided range and splits.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        insert_func = """CREATE OR REPLACE FUNCTION insert_alphas(s int, low double precision, up double precision) RETURNS void AS $$
                        DECLARE 
                         step_width double precision;
                         alpha double precision; 
                         aid int; 
                         insertion text;
                        BEGIN
                          step_width := (up - low) / s;
                          alpha := low;
                          aid := 1;
                          WHILE alpha <= up LOOP
                            INSERT INTO alphas VALUES (aid, alpha);
                            alpha := alpha + step_width;
                            aid := aid + 1;
                          END LOOP;
                        END;
                       $$ LANGUAGE PLPGSQL;
        """
        db.execute_engine_query(query=insert_func)
        db.execute_engine_query(query=
                """DO $$ 
                            BEGIN 
                            PERFORM insert_alphas("""
                + str(SPLITS)
                + """, 0, 180);
                            END;
                            $$;
                         """
            )
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed inserting alpha values: {e}")


def insert_first_permutation(db: Postgresql_DB_API):
    """
    Creates and executes the insert_first_permutation function.

    Description:
    Creates a PostgreSQL function to insert the first permutation into the 'next_permutation' table. This initializes the permutation process.

    Parameters:
    db (Postgresql_DB_API): The database API instance used for database operations.

    Returns:
    None
    """
    try:
        insert_permutation = """CREATE OR REPLACE FUNCTION insert_first_permutation() RETURNS void AS $$
                                DECLARE
                                    d int;
                                BEGIN
                                
                                d = (SELECT get_dimension() - 1);
                                INSERT INTO next_permutation VALUES (0, d);
                                FOR dimension in 1..d LOOP
                                    INSERT INTO next_permutation VALUES (dimension, 1);
                                END LOOP;
                                END;
                               $$ LANGUAGE PLPGSQL;   
        """
        db.execute_engine_query(query=insert_permutation)
        db.execute_engine_query(query=
                         """DO $$ 
                            BEGIN 
                            PERFORM insert_first_permutation();
                            END;
                            $$;
                         """)
    except Exception as e:
        logging.error(f"{datetime.now()} - Failed inserting first permutation: {e}")
    
