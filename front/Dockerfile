FROM ubuntu:latest

RUN apt-get update && apt-get install -y git nodejs npm

RUN ["git", "clone", "https://github.com/ArmandThomas/bigmac_inflation_study.git"]

WORKDIR /bigmac_inflation_study

RUN ["npm", "install"]

CMD ["npm", "start"]

