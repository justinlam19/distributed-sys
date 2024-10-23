build:
	docker build -t "dsys" .

run:
	docker run -it -v ~/cs426-fall24:/home/nonroot dsys /bin/bash

prune:
	docker system prune

buildception:
	docker build --progress plain -t registry.cs426.cloud/jl4268/video-rec-service:latest .

runception:
	docker run -it -v ~/cs426-fall24:/home/nonroot -v /var/run/docker.sock:/var/run/docker.sock dsys    /bin/bash

push:
	docker push registry.cs426.cloud/jl4268/video-rec-service:latest
	