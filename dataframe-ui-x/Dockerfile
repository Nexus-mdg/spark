# Stage 1: Build React (Vite + Tailwind)
FROM node:20-alpine AS web-build

WORKDIR /app/web

# Install deps first to leverage Docker layer caching
COPY web/package.json ./
RUN npm install

# Copy the rest of the frontend and build
COPY web/ .
RUN npm run build

# Stage 2: Flask server
FROM python:3.11-slim

WORKDIR /app

# Install python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy server and built frontend
COPY app.py ./
COPY --from=web-build /app/web/dist ./dist

EXPOSE 5001
ENV PORT=5001
CMD ["python", "app.py"]
