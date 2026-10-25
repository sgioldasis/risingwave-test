# Superset's MySQL engine spec imports MySQLdb (mysqlclient) unconditionally when it reads result
# column types. The StarRocks connections use mysql+pymysql, so let pymysql stand in for MySQLdb.
import pymysql

pymysql.install_as_MySQLdb()
