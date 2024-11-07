## Build the docs and generate a PDF

The Docker compose file 
- launches a service that builds the docs and serves them on port 3000
- launches a service that crawls the docs and generates PDF files for each page

```bash
docker compose up --detach --wait --wait-timeout 120 --build
```

