#!/bin/bash

echo "Enter airflow optim DAG start date in format: <year-month-day-hour-minute>"
read -r start_date
if [ -z "$start_date" ];
then
    echo "No mode parameter passed"
    exit
fi

echo "Enter airflow optim DAG restart interval in minutes: <minutes>"
read -r interval
if [ -z "$interval" ];
then
    echo "No token name passed"
    exit
fi

echo "Enter airflow optim DAG stop date interval in hours: <hours>"
read -r stop_date
if [ -z "$stop_date" ];
then
    echo "No email pass passed"
    exit
fi

echo "Enter email pass <pass>"
read -r email
if [ -z "$email" ];
then
    echo "No email pass passed"
    exit
fi

sudo docker run --rm -d -p 8080:8080\
 -e START_DATE="$start_date"\
 -e INTERVAL="$interval"\
 -e END_DATE_INTERVAL="$stop_date"\
 -e MAIL_PASS="$email"\
 -v /home/alext/alex/railoptim/data/:/opt/airflow/data:z\
 -v /home/alext/alex/railoptim/dags/:/opt/airflow/dags:z\
 --name optim optim:airflow