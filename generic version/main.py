#from Full_Load_ETL import GenericETL
from Incremental_Load import IncrementalETL
if __name__ == "__main__":
    #etl = GenericETL()
    etl1 = IncrementalETL()
    #etl.run()
    etl1.run()
