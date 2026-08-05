FROM python:3.12.7
LABEL maintainer="Rob Svirskas <svirskasr@hhmi.org>"
COPY bin/* /app/
COPY acknowledgements/bin/*py /app/
WORKDIR /app
RUN pip3 install -r requirements.txt
CMD ["/bin/bash"]
