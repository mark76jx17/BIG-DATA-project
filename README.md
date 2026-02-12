# The BIG-DATA Project

## Objective

Create an **interactive map** showing access to basic services for the cities of **Pavia** and **Cagliari** and other cities.

The map displays the **number of different basic services per H3 unit**  
(H3 documentation: https://www.uber.com/en-ES/blog/h3/).

The final visualization is implemented using **Kepler.gl**.
---

## Examples of Basic Services

### Health
- Hospital  
- Pharmacy  
- Dentist  
- Clinics  

### Education
- School  
- Kindergarten  
- University  
- Library  

### Food
- Supermarket  
- Market  

### Security
- Police  
- Fire station  

### Public Services
- Post office  
- City council  

### Sports
- Swimming pool  
- Gym  

---

## Workflow

1. **Download data from OpenStreetMap**
   - Osmx
   - Identify available services and categories

2. **Process the data**
   - Extract POIs and locations
   - Tools:
     - Python
     - PySpark

3. **Load into Apache Spark**
   - Compute H3 index for each POI
   - Group services per H3 cell

4. **Index with Elasticsearch**
   - Index by:
     - H3 cell
     - Service type (health, education, etc.)

5. **Visualization**
   - Use **Kepler.gl**
   - Display aggregated services per H3 hexagon

---

# How to Execute && Activate Elasticsearch

### Requirements
- Install **Docker**
- Install dependencies from `requirements.txt`

### Run Elasticsearch Container

```bash
sudo docker rm -f elasticsearch 2>/dev/null

sudo docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -m 1g \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
  -e http.cors.enabled=true \
  -e 'http.cors.allow-origin="/.*/"' \
  -e http.cors.allow-methods=OPTIONS,HEAD,GET,POST \
  -e http.cors.allow-headers=Content-Type \
  elasticsearch:8.17.0


python3 run_analysis.py

# or, if data is alredy downloaded  

python3 run_analysis.py --cached
```
to view the map
```bash

python3 output/launch_explorer.py

```
