FROM node:18.18.2

WORKDIR /app/docs

EXPOSE 3000

COPY scripts /app/docs/scripts/
COPY babel.config.js /app/docs/
COPY cli.js /app/docs/
COPY docs /app/docs/docs/
COPY docusaurus.config.js /app/docs/
COPY i18n /app/docs/i18n/
COPY package.json /app/docs/
COPY sidebars.js /app/docs/
COPY src /app/docs/src/
COPY static /app/docs/static/
COPY versioned_docs /app/docs/versioned_docs/
COPY versioned_sidebars /app/docs/versioned_sidebars/
COPY versions.json /app/docs/
COPY yarn.lock /app/docs/

RUN yarn install --frozen-lockfile
ENV NODE_OPTIONS="--max-old-space-size=8192"

