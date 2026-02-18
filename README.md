# Urban Map Explorer

![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-E25A1C?logo=apachespark&logoColor=white)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.17-005571?logo=elasticsearch&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-150458?logo=pandas&logoColor=white)
![kepler.gl](https://img.shields.io/badge/kepler.gl-0.3.2-2E86C1?logo=mapbox&logoColor=white)
![H3](https://img.shields.io/badge/H3-3.7%2B-1A5276?logo=hexo&logoColor=white)


We present an efficient, Big Data–ready workflow for downloading urban data from OpenStreetMap and presenting it through an intuitive, user-friendly interface. The platform enables interactive exploration, service selection, simple queries, and service filtering, combining a Kepler.gl–powered visualization with an integrated query tool.

---

## Workflow

### Download data from OpenStreetMap
   - Osmx API
   - Identify available services and categories for the selected cities

### Spark Data Processing
   - Spark session initialization
   - Service mapping into macro-categories
   - Computing of H3-indexes for geographic cell renedring
   - Aggregation of services by H3 cells
   - Computation of **Empirical Accessibility Index**

### ElasticSearch Index Creation
   - Index by:
      - H3 cell
      - Service type (health, education, etc.)

### Statistical analysis
   - Computation of insights for city accessibility comparison
   - Creation of syntethic descriptive reports

### Visualization
   - Creation of interactive map with **Kepler.gl** framework
   - Display aggregated and category-filtered services per H3 hexagon
   - Integration of intuitive query tool

## Services and POIs considered

### 🏥 Health
Access to medical care, healthcare services, and prevention.

**Examples**: Hospitals, Pharmacies, Medical/Dental Clinics.

### 📚 Education
Places dedicated to learning, research, and training.

**Examples**: Universities, Libraries, Language Schools.

### 🍕 Food
Services for consuming ready-made meals and dining socialization.

Examples: Canteens/Restaurants, Bars/Cafés, Pubs/Fast-food.

### 🛒 Food Retail
Shops for purchasing food and drinks for domestic consumption.

Examples: Supermarkets, Bakeries, Local Markets.

### 🛍️ Retail
General consumer goods stores, equipment, and department stores.

**Examples**: Shopping Malls, Newsstands, Hardware Stores.

### ✂️ Services
Activities dedicated to personal care and daily needs.

**Examples**: Hairdressers/Barbers, Laundromats, Beauty Centers.

### 💳 Financial
Money management and tax/economic consultancy.

**Examples**: ATMs, Banks, Accountants.

### 🏛️ Public Services
Administrative offices and state infrastructure for citizens.

**Examples**: Post Offices, Town Halls, Employment Agencies.

### 👮 Security
Facilities for citizen protection and emergency management.

**Examples**: Police Stations, Fire Stations.

### ⚽ Sports
Facilities for physical activity, training, and sporting events.

**Examples**: Gyms, Swimming Pools, Sports Fields/Stadiums.

### 🌳 Recreation
Green spaces and public areas for relaxation and leisure time.

**Examples**: Parks, Picnic Areas, Public Gardens.

### 🎭 Culture
Entertainment venues, artistic production, and nightlife.

**Examples**: Cinemas/Theaters, Museums, Nightclubs.

### ⛪ Religion
Places of worship and spaces dedicated to spirituality.

**Examples**: Churches/Mosques/Synagogues, Monasteries.

### 🚲 Transportation
Infrastructure for moving around the city or renting vehicles.

**Examples**: Bus Stops/Stations, Bike Sharing, Bicycle Parking.

### 🏨 Tourism
Hospitality, accommodation, and information services for visitors.

**Examples**: Hostels/Hotels, Info Points, Viewpoints.

### ⚖️ Professional Services
Legal, notary, and real estate brokerage consultancy.

**Examples**: Law Firms, Notaries, Real Estate Agencies.

### 🚮 Public Utilities
Essential services for urban decorum and primary needs.

**Examples**: Drinking Fountains, Public Toilets, Waste Collection/Recycling.

## Empirical Accessibility Index

To measure accessibility in a given urban area in a way that allows meaningful comparisons between different areas, we developed an empirical index based on survey data collected from our colleagues. The index is designed to be simple yet effective, as it captures the overall perceived utility of the services available in an urban area according to people’s preferences.

For each H3 cell, the index is computed using the following formula:

$$
EAI = \frac{1}{N}\sum{w_in_i}
$$

where 
- $N$: total number of services in the cell
- $w_i$: empirical weight for the $i$-th service
- $n_i$: occurrences of the $i$-th service in the cell

## Get started

To enable the use of 
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
