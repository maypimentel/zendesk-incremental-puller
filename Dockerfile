FROM python:3.7-alpine as baseImage
    ENV APPDIR /app
    WORKDIR ${APPDIR}
    ADD requirements.txt ${APPDIR}/requirements.txt
    RUN pip3 install -r ${APPDIR}/requirements.txt
    RUN adduser puller -S
    USER puller

    CMD ["python3", "zendesk-puller.py"]