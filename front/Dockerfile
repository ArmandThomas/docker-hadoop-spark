FROM timbru31/node-alpine-git

RUN ["git", "clone", "https://github.com/ArmandThomas/bigmac_inflation_study.git"]

WORKDIR /bigmac_inflation_study

RUN ["npm", "install"]

CMD ["npm", "start"]

