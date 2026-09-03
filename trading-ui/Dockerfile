FROM node:25-alpine AS build
WORKDIR /app
COPY trading-ui/package*.json ./
RUN npm install --ignore-scripts
COPY trading-ui/ ./
RUN npm run build

FROM nginx:1.27-alpine
COPY trading-ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget -q --spider http://localhost/ || exit 1
