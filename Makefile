APPDIR=/app
CONTAINER_NAME=zendesk-puller

build:
	docker build -t my/${CONTAINER_NAME} .

run: build
	docker run -it --rm --name ${CONTAINER_NAME} --env-file .env -v $(shell pwd):${APPDIR} my/${CONTAINER_NAME}
