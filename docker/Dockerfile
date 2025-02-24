#base image
FROM ubuntu:22.04

#working directory inside container
WORKDIR /p1_contanier

#necessary install for script 
RUN apt-get update && apt-get install -y wget gzip grep bash

#copy multi.sh script into container
COPY multi.sh download.sh ./

RUN chmod +x multi.sh download.sh

#rn script when container starts
CMD ["./multi.sh"]
