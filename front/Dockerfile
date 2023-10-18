FROM node:16-alpine3.11 AS builder

RUN ["git", "clone", "https://github.com/ArmandThomas/bigmac_inflation_study"]

WORKDIR /bigmac_inflation_study

RUN ["npm", "install"]

CMD ["npm", "run", "start"]
