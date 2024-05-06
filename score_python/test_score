from jinjasql import JinjaSql
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from urllib.parse import quote_plus
import pandas as pd
import sqlalchemy
from sshtunnel import SSHTunnelForwarder
import logging
import os


class Config(object):

    class Redshift:
        ssh_password = os.environ.get(env='REDSHIFT_SSH_PASSWORD_ADC')
        db_pwd = os.environ.get(env='REDSHIFT_DATABASE_PASSWORD_JENKINS')


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

ssh_user = "wallixusr@local@PPFGH1GTW03:SSH:ADECOS01"
ssh_host = "wallix-bastion-eu-pr.dktapp.cloud"
db_user = "jenkins_belgium"
db_name = "dvdbredshift02"

query_to_create_score = f"""INSERT INTO cds.d_seg_header
(
  shr_score_name,
  shr_international_flag,
  cnt_country_code,
  shr_creation_date,
  shr_updated_date,
  shr_expiration_date,
  shr_ideliver_flag,
  shr_istory_flag,
  shr_dmp_flag
)
VALUES
(
  'score_test_migration',
  0,
  'BE',
  SYSDATE,
  SYSDATE,
  '2999-12-31',
  1,
  0,
  1
);
DROP TABLE IF EXISTS score_name;
CREATE TEMP TABLE score_name
AS
SELECT *
FROM cds.d_seg_header
WHERE shr_score_name = 'score_test_migration';
-- Values creation
INSERT INTO cds.d_seg_value
(
  sve_score_value_name
)
VALUES
(
  'test_migration'
);
DROP TABLE IF EXISTS score_values;
CREATE TEMP TABLE score_values
AS
SELECT sve_idr_score_value,
       sve_score_value_name
FROM cds.d_seg_value
WHERE sve_score_value_name IN ('test_migration')
GROUP BY 1,
         2;
------------------------------------------------
DROP TABLE IF EXISTS customers_scores_values;

CREATE TEMP TABLE customers_scores_values DISTKEY (person_id) SORTKEY (person_id)
AS
SELECT
	member_id,
	CAST(
		person_id AS BIGINT
	),
	shared_id,
	'score_test_migration' AS score_name,
	'test_migration' AS value_name
FROM
	(
		SELECT
			member_id,
			shared_id,
			person_id
		FROM
			restricted_access.d_customers_be
		WHERE
			is_decathlonien = 1
		GROUP BY
			1,
			2,
			3
	);

INSERT INTO cds.f_seg_customer_segmentation
SELECT csv.member_id,
       csv.person_id,
       csv.shared_id,
       sn.shr_idr_score,
       sv.sve_idr_score_value,
       SYSDATE,
       1,
       sysdate,
       'score_test_migration_creation' as rs_technical_flow
FROM customers_scores_values csv
  INNER JOIN score_name sn ON csv.score_name = sn.shr_score_name
  INNER JOIN score_values sv ON csv.value_name = sv.sve_score_value_name
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9;"""


def get_redshift_connection(ssh_host: str, ssh_user: str, ssh_password: str,
                            database_user: str, database_password: str, database_name: str,
                            enable_ssh_connection: bool): #-> Union[MockConnection, None]:
    f"""
    get a redshift connector instantiated with ssh funnel
    
    :param ssh_host:                name of the ssh host you want to connect with
    :param ssh_user:                name of the ssh user that will serve the connection (could be replaced with any user 
                                    that has access to the host
    :param enable_ssh_connection:   boolean that indicates if we connect through SSH protocol or not
    :param ssh_password:            password associated for the given user to connect to the ssh_host
    :param database_user:           name of the user that has access to the database you want to retrieve information
    :param database_password:       password associated for the given user to connect to the redshift database
    :param database_name:           name of the redshift database you want to retrieve information
    
    :return:                        a MockConnection with a sqlalchemy engine that connect to a redshift database, or 
                                    None if no password were found. 
    
    :raise:                     ValueError: when ssh password and/or redshift database password were not found neither 
                                in the configuration file or on given environment variables 
                                (resp. REDSHIFT_DATABASE_PASSWORD, REDSHIFT_SSH_PASSWORD)
    """
    try:
        if enable_ssh_connection:
            server = SSHTunnelForwarder(
                ssh_host,
                ssh_username=ssh_user,
                ssh_password=ssh_password,
                remote_bind_address=("redshift.dktapp.cloud", int(5539)),
            )
            server.start()
            logger.info(f"Server connected via SSH || Local Port: {server.local_bind_port}...")
            execution_options = {"isolatio  n_level": "AUTOCOMMIT"}
            host = f"localhost:{server.local_bind_port}"
            conn_string = f"postgresql://{database_user}:{quote_plus(database_password)}@{host}/{database_name}"
            engine = create_engine(
                conn_string,
                poolclass=NullPool,
                execution_options=execution_options,
            )
            logger.info(f"redshift connection established successfully !")
        else:
            engine = create_engine(
                f"postgresql://{database_user}:{quote_plus(database_password)}@redshift.dktapp.cloud:5539/{database_name}"
                )
            logger.info(f"redshift connection established successfully !")

        return engine.connect()
    except ValueError:
        logger.error("no password specified in REDSHIFT_SSH_PASSWORD and/or REDSHIFT_DATABASE_PASSWORD or in the "
                     "configuration file")
        return None
    




def execute_query(conn):
    f"""
    get a pandas dataframe with all transactions joined with product information

    :return:    a pandas dataframe with all transactions for given touchpoints

    :raise:     OperationalError: when the query takes too long or deals with too much data. In this case, we are
                restarting the query.
    """
    query: str = query_to_create_score
      
    params = {}
    j = JinjaSql()
    template = query.replace('}}', ' | sqlsafe }}')
    query = j.prepare_query(template, params)[0]

    conn.execute(query)
    logger.info(
        f"data retrieved")
    return None
    
connection = get_redshift_connection(ssh_host=ssh_host, ssh_user=ssh_user, ssh_password=ssh_password,
                            database_user=db_user, database_password= db_pwd, database_name=db_name,
                            enable_ssh_connection=True)

execute_query(conn=connection)  