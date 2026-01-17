table = "victimtable"
db_name = "flask_db"

control = utils.MariaDBController(
    db_name,
    host="mariadb_container",
    port=3306,
    datadir="/var/lib/mysql",
    container_name="mariadb_container",
    container_datadir="/var/lib/mysql",
)