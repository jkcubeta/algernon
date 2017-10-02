docker build --no-cache -f algernon/build/dockerfiles/api_Dockerfile -t lane-docker.algernon.lan/alg_api:0.3 algernon
docker push lane-docker.algernon.lan/alg_api:0.3
docker build --no-cache -f algernon/build/dockerfiles/Dockerfile_lane -t lane-docker.algernon.lan/lane:0.1 algernon
docker push lane-docker.algernon.lan/lane:0.1