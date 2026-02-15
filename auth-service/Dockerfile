FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev || npm install --production

COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 3000

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
