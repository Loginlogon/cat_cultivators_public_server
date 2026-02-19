FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --production

COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 3002

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]

