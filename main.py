from src.db_api import Postgresql_DB_API as db_api
import src.utils as utils
import src.settings as settings
from src.cash import CASH

# The name of the table storing input data
data_table_name = "data"


def main():
    # Start Error Logging 
    utils.start_logging()

    # Build connection and create cash database
    db = db_api()
    db.connect()
    db.create_database(db_name=settings.DBNAME)

    # Read Data, create tables and init tables
    utils.create_table_from_csv(db=db, table_name=data_table_name)
    utils.init_helper_functions(db=db)
    utils.create_tables(db=db)
    utils.create_cash_functions(db=db)
    utils.init_tables(db=db)

    # CASH Algorithm
    cash = CASH()
    cash.fit(db=db)

    # Print Result
    result = db.execute_engine_query("SELECT cluster FROM clusters")
    print("----------------------Clusters Found----------------------")
    
    for cluster in result:
        print("Cluster: " + str(cluster))

    print("----------------------------------------------------------")

    return result

if __name__ == "__main__":
    main()
