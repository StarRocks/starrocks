FROM node:24

WORKDIR /app/docusaurus
ENV NODE_OPTIONS="--max-old-space-size=9216 --no-warnings=ExperimentalWarning"
ENV DISABLE_VERSIONING=true
ENV DOCUSAURUS_IGNORE_SSG_WARNINGS=true

EXPOSE 3000

COPY . .

RUN yarn install --frozen-lockfile

