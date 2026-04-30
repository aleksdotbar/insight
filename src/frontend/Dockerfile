FROM node:24-bookworm-slim AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

COPY . .
RUN npm run build

FROM nginx:1.27-alpine

# `envsubst` is provided by gettext; the official nginx:alpine image already
# includes it. We list it here defensively in case the base image ever drops it.
RUN apk add --no-cache gettext

COPY --from=builder /app/dist /usr/share/nginx/html
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# nginx config: security-headers snippet (included at server level AND inside
# any location block that declares its own add_header — nginx replaces rather
# than merges across levels) + default.conf template that the entrypoint
# renders at container start (envsubst on ${CSP_REMOTE}).
RUN mkdir -p /etc/nginx/snippets
COPY nginx/security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template

EXPOSE 80

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
