cd /home/nurgun/Documents/homework_mar_14

docker logs jupyter | grep -i token

sudo chmod 777 -R ./notebooks

docker logs jupyter 2>&1 | grep "http://127.0.0.1:8888"
