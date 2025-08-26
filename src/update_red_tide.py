import requests
import json
import os
import re
import time
from datetime import datetime, timezone
import pytz
import gspread
from google.oauth2.service_account import Credentials

class HierarchicalRedTideProcessor:
    def __init__(self):
        self.fwc_api_url = "https://atoll.floridamarine.org/arcgis/rest/services/FWC_GIS/OpenData_HAB/MapServer/9/query"
        self.wp_site_url = os.environ['WORDPRESS_SITE_URL']
        self.wp_username = os.environ['WORDPRESS_USERNAME'] 
        self.wp_password = os.environ['WORDPRESS_APP_PASSWORD']
        
        # Initialize Google Sheets
        self._init_google_sheets()
        
        # Load data from Google Sheets
        self.locations_data = self._load_locations()
        self.sample_mapping = self._load_sample_mapping()
        
        # Track WordPress post relationships
        self.wp_posts = {
            'region': {},
            'city': {},
            'beach': {}
        }
    
    def _init_google_sheets(self):
        """Initialize Google Sheets client"""
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        creds_dict = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        self.sheets_client = gspread.authorize(creds)
        self.sheet = self.sheets_client.open_by_key(os.environ['GOOGLE_SHEET_ID'])
    
    def _load_locations(self):
        """Load beach locations from Google Sheets"""
        try:
            worksheet = self.sheet.worksheet('locations')
            records = worksheet.get_all_records()
            
            # Organize by location type
            locations = {
                'beaches': {},
                'cities': {},
                'regions': {}
            }
            
            for record in records:
                beach_name = record['beach']
                city = record['city']
                region = record['region']
                
                # Store beach data
                locations['beaches'][beach_name] = record
                
                # Track unique cities
                if city not in locations['cities']:
                    locations['cities'][city] = {
                        'city': city,
                        'region': region,
                        'beaches': [],
                        'state': record.get('state', 'FL')
                    }
                locations['cities'][city]['beaches'].append(beach_name)
                
                # Track unique regions  
                if region not in locations['regions']:
                    locations['regions'][region] = {
                        'region': region,
                        'cities': set(),
                        'beaches': [],
                        'state': record.get('state', 'FL')
                    }
                locations['regions'][region]['cities'].add(city)
                locations['regions'][region]['beaches'].append(beach_name)
            
            # Convert sets to lists for JSON serialization
            for region_data in locations['regions'].values():
                region_data['cities'] = list(region_data['cities'])
            
            print(f"Loaded {len(locations['beaches'])} beaches, {len(locations['cities'])} cities, {len(locations['regions'])} regions")
            return locations
        except Exception as e:
            print(f"Error loading locations: {e}")
            return {'beaches': {}, 'cities': {}, 'regions': {}}
    
    def _load_sample_mapping(self):
        """Load HAB sampling site mappings from Google Sheets"""
        try:
            worksheet = self.sheet.worksheet('sample_mapping')
            records = worksheet.get_all_records()
            
            # Group by beach name
            mapping = {}
            for record in records:
                beach_name = record['beach']
                if beach_name not in mapping:
                    mapping[beach_name] = []
                mapping[beach_name].append(record)
            
            print(f"Loaded sample mappings for {len(mapping)} beaches")
            return mapping
        except Exception as e:
            print(f"Error loading sample mapping: {e}")
            return {}
    
    def generate_slug(self, location_name, location_type):
        """Generate SEO-friendly slug with red-tide prefix"""
        # Clean the location name
        clean_name = location_name.lower()
        clean_name = re.sub(r'[^a-z0-9\s-]', '', clean_name)  # Remove special chars
        clean_name = re.sub(r'\s+', '-', clean_name)  # Replace spaces with hyphens
        clean_name = re.sub(r'-+', '-', clean_name)   # Remove multiple hyphens
        clean_name = clean_name.strip('-')            # Remove leading/trailing hyphens
        
        return f"red-tide-{clean_name}"
    
    def parse_abundance_number(self, abundance_text):
        """Extract numerical cell count from FWC abundance categories"""
        abundance_lower = abundance_text.lower()
        
        # Extract numbers from parentheses
        numbers = re.findall(r'[\d,]+', abundance_text)
        
        if 'not present' in abundance_lower or 'background' in abundance_lower:
            return 500, 'safe'
        elif 'very low' in abundance_lower:
            return 2500, 'safe'
        elif 'low' in abundance_lower and 'very' not in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'caution'
            return 5000, 'caution'
        elif 'medium' in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'avoid'
            return 50000, 'avoid'
        elif 'high' in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'avoid'
            return 500000, 'avoid'
        
        return 0, 'safe'
    
    def calculate_beach_status(self, sampling_sites, fwc_data):
        """Calculate overall beach status from multiple HAB sampling sites with distance weighting"""
        
        if not sampling_sites:
            return {
                'status': 'no_data',
                'count': 0,
                'confidence': 0,
                'sample_date': None,
                'sampling_sites': []
            }
        
        # Weight factors based on distance
        def get_distance_weight(distance):
            if distance <= 1.0:
                return 1.0      # Full weight for sites within 1 mile
            elif distance <= 3.0:
                return 0.7      # 70% weight for 1-3 miles
            elif distance <= 10.0:
                return 0.4      # 40% weight for 3-10 miles
            else:
                return 0.2      # 20% weight for sites over 10 miles
        
        site_results = []
        weighted_scores = []
        latest_sample_date = None
        
        # Process each sampling site
        for site in sampling_sites:
            hab_id = site['HAB_id']
            distance = float(site['sample_distance'])
            
            # Find matching FWC data
            site_data = self._find_hab_data_by_id(fwc_data, hab_id, site['sample_location'])
            
            if site_data:
                cell_count, status = self.parse_abundance_number(site_data['abundance'])
                sample_date = datetime.fromtimestamp(site_data['sample_date'] / 1000)
                
                # Update latest sample date
                if not latest_sample_date or sample_date > latest_sample_date:
                    latest_sample_date = sample_date
                
                # Calculate weighted score (higher = worse conditions)
                status_score = {'safe': 0, 'caution': 1, 'avoid': 2}.get(status, 0)
                distance_weight = get_distance_weight(distance)
                age_days = (datetime.now() - sample_date).days
                
                # Reduce weight for older samples (beyond 7 days)
                age_weight = max(0.1, 1 - (age_days / 7.0)) if age_days > 7 else 1.0
                
                final_weight = distance_weight * age_weight
                weighted_score = status_score * final_weight
                weighted_scores.append(weighted_score)
                
                site_results.append({
                    'hab_id': hab_id,
                    'sample_location': site['sample_location'],
                    'distance_miles': distance,
                    'current_concentration': cell_count,
                    'status_contribution': 'primary' if distance <= 1.0 else 'secondary' if distance <= 3.0 else 'reference',
                    'sample_date': sample_date.strftime('%Y-%m-%d'),
                    'raw_abundance': site_data['abundance'],
                    'weight': final_weight
                })
        
        # Calculate overall status
        if not weighted_scores:
            overall_status = 'no_data'
            confidence = 0
            peak_count = 0
        else:
            # Use weighted average to determine overall status
            avg_weighted_score = sum(weighted_scores) / len(weighted_scores)
            total_weight = sum([s['weight'] for s in site_results])
            
            if avg_weighted_score >= 1.5:
                overall_status = 'avoid'
            elif avg_weighted_score >= 0.5:
                overall_status = 'caution'
            else:
                overall_status = 'safe'
            
            # Confidence based on number of sites and recency
            confidence = min(100, int(total_weight * 50 + len(site_results) * 10))
            peak_count = max([s['current_concentration'] for s in site_results])
        
        return {
            'status': overall_status,
            'count': peak_count,
            'confidence': confidence,
            'sample_date': latest_sample_date,
            'sampling_sites': site_results
        }
    
    def _find_hab_data_by_id(self, fwc_data, hab_id, sample_location):
        """Find FWC data by HAB ID or location name matching"""
        # First try exact HAB ID match
        for feature in fwc_data['features']:
            attrs = feature['attributes']
            if attrs.get('HAB_ID') == hab_id:
                return {
                    'abundance': attrs.get('Abundance', 'No Data'),
                    'sample_date': attrs.get('SAMPLE_DATE'),
                    'location': attrs.get('LOCATION'),
                    'latitude': attrs.get('LATITUDE'),
                    'longitude': attrs.get('LONGITUDE')
                }
        
        # Fallback: match by location name similarity
        sample_location_lower = sample_location.lower()
        best_match = None
        best_score = 0
        
        for feature in fwc_data['features']:
            attrs = feature['attributes']
            location = attrs.get('LOCATION', '').lower()
            
            # Simple similarity scoring
            if sample_location_lower in location or location in sample_location_lower:
                # Prioritize more recent samples
                sample_date = datetime.fromtimestamp(attrs['SAMPLE_DATE'] / 1000)
                age_days = (datetime.now() - sample_date).days
                score = max(0, 10 - age_days)
                
                if score > best_score:
                    best_score = score
                    best_match = attrs
        
        if best_match:
            return {
                'abundance': best_match.get('Abundance', 'No Data'),
                'sample_date': best_match.get('SAMPLE_DATE'),
                'location': best_match.get('LOCATION'),
                'latitude': best_match.get('LATITUDE'),
                'longitude': best_match.get('LONGITUDE')
            }
        
        return None
    
    def process_beach(self, beach_name, fwc_data):
        """Process a single beach"""
        print(f"Processing beach: {beach_name}...")
        
        # Get sampling sites for this beach
        sampling_sites = self.sample_mapping.get(beach_name, [])
        
        if not sampling_sites:
            print(f"  No sampling sites found for {beach_name}")
            return None
        
        print(f"  Found {len(sampling_sites)} sampling sites")
        
        # Calculate status using weighted approach
        result = self.calculate_beach_status(sampling_sites, fwc_data)
        
        # Get location data
        beach_data = self.locations_data['beaches'].get(beach_name, {})
        
        # Combine results
        processed_data = {
            'location_name': beach_name,
            'location_type': 'beach',
            'slug': self.generate_slug(beach_name, 'beach'),
            'current_status': result['status'],
            'peak_count': result['count'],
            'confidence_score': result['confidence'],
            'sample_date': result['sample_date'].strftime('%Y-%m-%d') if result['sample_date'] else None,
            'last_updated': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S'),
            'sampling_sites': result['sampling_sites'],
            'region': beach_data.get('region', ''),
            'city': beach_data.get('city', ''),
            'latitude': beach_data.get('latitude'),
            'longitude': beach_data.get('longitude'),
            'address': beach_data.get('address', ''),
            'state': beach_data.get('state', ''),
            'zip_code': beach_data.get('zip', '')
        }
        
        print(f"  Status: {result['status']} (confidence: {result['confidence']}%)")
        print(f"  Peak count: {result['count']} cells/L")
        
        return processed_data
    
    def process_city(self, city_name, beach_results):
        """Process city-level aggregation"""
        print(f"Processing city: {city_name}...")
        
        city_data = self.locations_data['cities'].get(city_name, {})
        city_beaches = city_data.get('beaches', [])
        
        # Filter beach results for this city
        relevant_beaches = [b for b in beach_results if b and b['city'] == city_name]
        
        if not relevant_beaches:
            print(f"  No beach data found for {city_name}")
            return None
        
        # Calculate city-wide metrics
        all_counts = [b['peak_count'] for b in relevant_beaches if b['peak_count'] > 0]
        all_confidences = [b['confidence_score'] for b in relevant_beaches]
        
        # Determine worst status among beaches
        status_priority = {'avoid': 3, 'caution': 2, 'safe': 1, 'no_data': 0}
        worst_status = 'safe'
        for beach in relevant_beaches:
            if status_priority[beach['current_status']] > status_priority[worst_status]:
                worst_status = beach['current_status']
        
        city_processed = {
            'location_name': city_name,
            'location_type': 'city',
            'slug': self.generate_slug(city_name, 'city'),
            'current_status': worst_status,
            'peak_count': max(all_counts) if all_counts else 0,
            'avg_count': int(sum(all_counts) / len(all_counts)) if all_counts else 0,
            'confidence_score': int(sum(all_confidences) / len(all_confidences)) if all_confidences else 0,
            'beach_count': len(relevant_beaches),
            'beaches_safe': len([b for b in relevant_beaches if b['current_status'] == 'safe']),
            'beaches_caution': len([b for b in relevant_beaches if b['current_status'] == 'caution']),
            'beaches_avoid': len([b for b in relevant_beaches if b['current_status'] == 'avoid']),
            'last_updated': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S'),
            'region': city_data.get('region', ''),
            'state': city_data.get('state', 'FL'),
            'child_beaches': [b['location_name'] for b in relevant_beaches]
        }
        
        print(f"  City status: {worst_status}")
        print(f"  Beaches: {len(relevant_beaches)} total")
        
        return city_processed
    
    def process_region(self, region_name, beach_results, city_results):
        """Process region-level aggregation"""
        print(f"Processing region: {region_name}...")
        
        region_data = self.locations_data['regions'].get(region_name, {})
        
        # Filter results for this region
        relevant_beaches = [b for b in beach_results if b and b['region'] == region_name]
        relevant_cities = [c for c in city_results if c and c['region'] == region_name]
        
        if not relevant_beaches:
            print(f"  No beach data found for {region_name}")
            return None
        
        # Calculate region-wide metrics
        all_counts = [b['peak_count'] for b in relevant_beaches if b['peak_count'] > 0]
        all_confidences = [b['confidence_score'] for b in relevant_beaches]
        
        # Determine worst status among beaches
        status_priority = {'avoid': 3, 'caution': 2, 'safe': 1, 'no_data': 0}
        worst_status = 'safe'
        for beach in relevant_beaches:
            if status_priority[beach['current_status']] > status_priority[worst_status]:
                worst_status = beach['current_status']
        
        region_processed = {
            'location_name': region_name,
            'location_type': 'region',
            'slug': self.generate_slug(region_name, 'region'),
            'current_status': worst_status,
            'peak_count': max(all_counts) if all_counts else 0,
            'avg_count': int(sum(all_counts) / len(all_counts)) if all_counts else 0,
            'confidence_score': int(sum(all_confidences) / len(all_confidences)) if all_confidences else 0,
            'beach_count': len(relevant_beaches),
            'city_count': len(relevant_cities),
            'beaches_safe': len([b for b in relevant_beaches if b['current_status'] == 'safe']),
            'beaches_caution': len([b for b in relevant_beaches if b['current_status'] == 'caution']),
            'beaches_avoid': len([b for b in relevant_beaches if b['current_status'] == 'avoid']),
            'last_updated': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S'),
            'state': region_data.get('state', 'FL'),
            'child_cities': [c['location_name'] for c in relevant_cities],
            'child_beaches': [b['location_name'] for b in relevant_beaches]
        }
        
        print(f"  Region status: {worst_status}")
        print(f"  Cities: {len(relevant_cities)}, Beaches: {len(relevant_beaches)}")
        
        return region_processed
    
    def create_or_update_wordpress_post(self, location_data, parent_post_id=None):
        """Create or update WordPress post for any location type"""
        
        slug = location_data['slug']
        location_type = location_data['location_type']
        location_name = location_data['location_name']
        
        # Search for existing post by slug
        search_url = f"{self.wp_site_url}/wp-json/wp/v2/red_tide_location"
        search_params = {'slug': slug}
        
        auth = (self.wp_username, self.wp_password)
        search_response = requests.get(search_url, params=search_params, auth=auth)
        
        if search_response.status_code == 200 and search_response.json():
            # Update existing post
            post_id = search_response.json()[0]['id']
            update_url = f"{self.wp_site_url}/wp-json/wp/v2/red_tide_location/{post_id}"
            method = 'POST'
            print(f"  Updating existing {location_type}: {location_name}")
        else:
            # Create new post
            update_url = f"{self.wp_site_url}/wp-json/wp/v2/red_tide_location"
            method = 'POST'
            print(f"  Creating new {location_type}: {location_name}")
        
        # Prepare title and content based on location type
        if location_type == 'beach':
            title = f"{location_name} Red Tide Status - Current Conditions & Updates"
            meta_description = f"Current red tide conditions at {location_name}. Real-time HAB monitoring data, safety information, and beach status updates."
        elif location_type == 'city':
            title = f"{location_name} Red Tide Status - All Beaches Current Conditions"
            meta_description = f"Red tide conditions for all beaches in {location_name}, FL. Current status, safety advisories, and detailed monitoring data."
        else:  # region
            title = f"{location_name} Red Tide Status - Regional Overview & Beach Conditions"
            meta_description = f"Comprehensive red tide monitoring for {location_name}. Track conditions across all beaches and cities in the region."
        
        # Prepare ACF data
        acf_data = {
            'location_name': location_name,
            'location_type': location_type,
            'location_slug': slug,
            'current_status': location_data['current_status'],
            'status_color': self._get_status_color(location_data['current_status']),
            'last_updated': location_data['last_updated']
        }
        
        # Add type-specific fields
        if location_type == 'beach':
            acf_data.update({
                'region': location_data['region'],
                'city': location_data['city'],
                'coordinates': f"{location_data['latitude']}, {location_data['longitude']}" if location_data['latitude'] else '',
                'full_address': location_data['address'],
                'state': location_data['state'],
                'zip_code': location_data['zip_code'],
                'peak_count': location_data['peak_count'],
                'confidence_score': location_data['confidence_score'],
                'sample_date': location_data['sample_date'],
                'sampling_sites': location_data['sampling_sites']
            })
        
        elif location_type in ['city', 'region']:
            acf_data.update({
                'peak_count': location_data['peak_count'],
                'avg_count': location_data['avg_count'],
                'confidence_score': location_data['confidence_score'],
                'beach_count': location_data['beach_count'],
                'beaches_safe': location_data['beaches_safe'],
                'beaches_caution': location_data['beaches_caution'],
                'beaches_avoid': location_data['beaches_avoid'],
                'state': location_data['state']
            })
            
            if location_type == 'region':
                acf_data['city_count'] = location_data['city_count']
        
        # WordPress payload
        payload = {
            'title': title,
            'slug': slug,
            'status': 'publish',
            'acf': acf_data,
            'yoast_meta': {
                'yoast_wpseo_metadesc': meta_description
            }
        }
        
        # Add parent relationship if provided
        if parent_post_id:
            payload['parent'] = parent_post_id
        
        # Make request
        headers = {'Content-Type': 'application/json'}
        response = requests.request(method, update_url, 
                                  json=payload, auth=auth, headers=headers)
        
        if response.status_code in [200, 201]:
            post_data = response.json()
            post_id = post_data['id']
            print(f"  WordPress success: {location_type} '{location_name}' (ID: {post_id})")
            
            # Store post ID for parent relationships
            self.wp_posts[location_type][location_name] = post_id
            return post_id
        else:
            print(f"  WordPress failed: {response.status_code} - {response.text}")
            return None
    
    def _get_status_color(self, status):
        """Get color code for status"""
        colors = {
            'safe': '#28a745',      # Green
            'caution': '#ffc107',   # Yellow
            'avoid': '#dc3545',     # Red
            'no_data': '#6c757d'    # Gray
        }
        return colors.get(status, '#6c757d')
    
    def update_google_sheets(self, all_processed_data):
        """Update Google Sheets with processed data"""
        try:
            # Update beach_status sheet
            status_worksheet = self.sheet.worksheet('beach_status')
            
            # Clear existing data and add headers
            status_worksheet.clear()
            status_headers = [
                'location_name', 'location_type', 'date', 'current_status', 
                'peak_count', 'confidence_score', 'sample_date', 'last_updated',
                'region', 'city', 'slug'
            ]
            status_worksheet.append_row(status_headers)
            time.sleep(1)
            
            # Add all processed data
            today = datetime.now().strftime('%Y-%m-%d')
            for item in all_processed_data:
                row = [
                    item['location_name'],
                    item['location_type'],
                    today,
                    item['current_status'],
                    item.get('peak_count', 0),
                    item.get('confidence_score', 0),
                    item.get('sample_date', ''),
                    item['last_updated'],
                    item.get('region', ''),
                    item.get('city', ''),
                    item['slug']
                ]
                status_worksheet.append_row(row)
                time.sleep(1.5)
            
            print(f"Updated beach_status sheet with {len(all_processed_data)} locations")
            
        except Exception as e:
            print(f"Google Sheets update error: {e}")
    
    def fetch_fwc_data(self):
        """Fetch latest data from FWC API"""
        params = {
            'where': '1=1',
            'outFields': '*',
            'outSR': '4326', 
            'f': 'json',
            'orderByFields': 'SAMPLE_DATE DESC'
        }
        
        response = requests.get(self.fwc_api_url, params=params)
        response.raise_for_status()
        return response.json()
    
    def run(self):
        """Main processing function with hierarchical structure"""
        print("Starting hierarchical red tide processing...")
        
        # Test WordPress authentication
        test_url = f"{self.wp_site_url}/wp-json/wp/v2/users/me"
        auth = (self.wp_username, self.wp_password)
        test_response = requests.get(test_url, auth=auth)
        
        if test_response.status_code != 200:
            print(f"WordPress auth failed: {test_response.text}")
            return
        else:
            print(f"Authenticated as: {test_response.json().get('name')}")
        
        # Fetch FWC data
        fwc_data = self.fetch_fwc_data()
        print(f"Fetched {len(fwc_data['features'])} HAB samples")
        
        # Process all locations hierarchically
        all_processed_data = []
        
        # 1. Process beaches first
        print("\n=== PROCESSING BEACHES ===")
        beach_results = []
        beaches_to_process = list(self.sample_mapping.keys())
        
        for beach_name in beaches_to_process:
            beach_data = self.process_beach(beach_name, fwc_data)
            if beach_data:
                beach_results.append(beach_data)
                all_processed_data.append(beach_data)
                
                # Create/update WordPress post
                self.create_or_update_wordpress_post(beach_data)
                time.sleep(2)
        
        # 2. Process cities
        print("\n=== PROCESSING CITIES ===")
        city_results = []
        cities_to_process = list(self.locations_data['cities'].keys())
        
        for city_name in cities_to_process:
            city_data = self.process_city(city_name, beach_results)
            if city_data:
                city_results.append(city_data)
                all_processed_data.append(city_data)
                
                # Create/update WordPress post
                self.create_or_update_wordpress_post(city_data)
                time.sleep(2)
        
        # 3. Process regions
        print("\n=== PROCESSING REGIONS ===")
        regions_to_process = list(self.locations_data['regions'].keys())
        
        for region_name in regions_to_process:
            region_data = self.process_region(region_name, beach_results, city_results)
            if region_data:
                all_processed_data.append(region_data)
                
                # Create/update WordPress post
                self.create_or_update_wordpress_post(region_data)
                time.sleep(2)
        
        # Update Google Sheets
        self.update_google_sheets(all_processed_data)
        
        print(f"\nProcessing complete!")
        print(f"Created/updated {len(beach_results)} beaches, {len(city_results)} cities, {len(regions_to_process)} regions")

if __name__ == "__main__":
    processor = HierarchicalRedTideProcessor()
    processor.run()