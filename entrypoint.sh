#!/bin/bash
set -e
 
airflow db init
 
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin

mysql -h mysql -u root -p$MYSQL_ROOT_PASSWORD -e "CREATE DATABASE IF NOT EXISTS github_event_analysis;"
mysql -h mysql -u root -p$MYSQL_ROOT_PASSWORD -e "CREATE USER IF NOT EXISTS 'samboy_88'@'%' IDENTIFIED BY 'awesome_person';"
mysql -h mysql -u root -p$MYSQL_ROOT_PASSWORD -e "GRANT ALL PRIVILEGES ON github_event_analysis.* TO 'samboy_88'@'%';"