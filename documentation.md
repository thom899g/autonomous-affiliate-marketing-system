## Data Collection and Storage Module Documentation

### Overview
The Data Collection and Storage module is responsible for gathering structured data related to affiliate marketing campaigns, including clickstream data, conversion tracking, and transactional information. The system uses a combination of Google Analytics API and third-party APIs to collect this data, which is then stored in Google BigQuery for further processing and analysis.

### Key Components

1. **DataCollector Class**
   - **Purpose**: Collects raw data from various sources.
   - **Initialization**: Uses configuration parameters to initialize clients for Google Analytics and other data sources.
   - **Methods**:
     - `collect_click_data()`: Fetches click data from Google Analytics API.
     - `collect_conversion_data()`: Fetches conversion data from affiliate networks.

2. **DataLoader Class**
   - **Purpose**: