cd /home/nurgun/Documents/homework_mar_14

docker logs jupyter | grep -i token

sudo chmod 777 -R ./notebooks
http://127.0.0.1:8888/lab

docker logs jupyter 2>&1 | grep "http://127.0.0.1:8888"


sudo lsof -i :3000

docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

docker exec -it jupyter bash wget 'url'
docker logs metabase 2>&1 | tail -40



docker volume rm feb14_pgdata_mydb

---ariflow eger metaya qosulmursa bunurun et
GRANT USAGE ON SCHEMA public TO airflow;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO airflow;