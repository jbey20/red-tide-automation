import requests
import json
import os
import re
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

class RedTideProcessor:
    def __init__(self):
        self.fwc_api_url = "https://atoll.floridamarine.org/arcgis/rest/services/FWC_GIS/OpenData_HAB/MapServer/9/query"
        self.wp_site_url = os.environ['WORDPRESS_SITE_URL']
        self.wp_username = os.environ['WORDPRESS_USERNAME'] 
        self.wp_password = os.environ['WORDPRESS_APP_PASSWORD']
        
        # Load beach mapping
        with open('config/beach_mapping.json', 'r') as f:
            self.beach_mapping = json.load(f)
    
    def parse_abundance_number(self, abundance_text):
        """Extract numerical cell count from FWC abundance categories"""
        abundance_lower = abundance_text.lower()
        
        # Extract numbers from parentheses
        numbers = re.findall(r'[\d,]+', abundance_text)
        
        if 'not present' in abundance_lower or 'background' in abundance_lower:
            return 500, 'Clear'
        elif 'very low' in abundance_lower:
            return 2500, 'Clear'
        elif 'low' in abundance_lower and 'very' not in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'Low'
            return 5000, 'Low'
        elif 'medium' in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'Medium'
            return 50000, 'Medium'
        elif 'high' in abundance_lower:
            if len(numbers) >= 2:
                low = int(numbers[0].replace(',', ''))
                high = int(numbers[1].replace(',', ''))
                return (low + high) // 2, 'High'
            return 500000, 'High'
        
        return 0, 'Clear'
    
    def find_beach_data(self, fwc_data, beach_locations):
        """Find most recent abundance data for specific beach"""
        best_match = None
        best_score = 0
        
        for feature in fwc_data['features']:
            attrs = feature['attributes']
            location = attrs.get('LOCATION', '').lower()
            
            for fwc_location in beach_locations:
                if fwc_location.lower() in location:
                    sample_date = datetime.fromtimestamp(attrs['SAMPLE_DATE'] / 1000)
                    age_days = (datetime.now() - sample_date).days
                    score = max(0, 10 - age_days)
                    
                    if score > best_score:
                        best_score = score
                        best_match = attrs
        
        if best_match:
            abundance_text = best_match['Abundance']
            cell_count, status = self.parse_abundance_number(abundance_text)
            return {
                'status': status,
                'count': cell_count,
                'raw_abundance': abundance_text,
                'location': best_match.get('LOCATION'),
                'sample_date': datetime.fromtimestamp(best_match['SAMPLE_DATE'] / 1000),
                'latitude': best_match.get('LATITUDE'),
                'longitude': best_match.get('LONGITUDE')
            }
        
        return {
            'status': 'No Data',
            'count': 0,
            'raw_abundance': 'No recent samples',
            'location': None,
            'sample_date': None,
            'latitude': None,
            'longitude': None
        }
    
    def process_beach_page(self, page_key, page_config, fwc_data):
        """Process abundance data for one beach page"""
        beach_data = {}
        all_counts = []
        
        # Process each beach
        for i in range(1, 5):
            beach_key = f"beach_{i}"
            locations = page_config['beaches'].get(f'{beach_key}_fwc_locations', [])
            
            data = self.find_beach_data(fwc_data, locations)
            
            beach_data[f'{beach_key}_status'] = data['status']
            beach_data[f'{beach_key}_count'] = data['count']
            beach_data[f'{beach_key}_name'] = page_config['beaches'].get(f'{beach_key}_name', '')
            
            if data['count'] > 0:
                all_counts.append(data['count'])
        
        # Calculate overall metrics
        if all_counts:
            beach_data['peak_count'] = max(all_counts)
            beach_data['avg_count'] = int(sum(all_counts) / len(all_counts))
        else:
            beach_data['peak_count'] = 0
            beach_data['avg_count'] = 0
        
        # Overall status based on highest count
        max_count = beach_data['peak_count']
        if max_count >= 100000:
            overall_status = 'High'
        elif max_count >= 10000:
            overall_status = 'Medium'
        elif max_count >= 1000:
            overall_status = 'Low'
        else:
            overall_status = 'Clear'
        
        beach_data['overall_status'] = overall_status
        beach_data['last_updated'] = datetime.now().strftime('%m/%d/%Y %I:%M %p')
        
        return beach_data
    
    def update_wordpress_page(self, page_config, beach_data):
        """Update WordPress with status and count data"""
        wp_api_url = f"{self.wp_site_url}/wp-json/wp/v2/beach_city/{page_config['post_id']}"
        
        acf_data = {
            'beach_1_status': beach_data['beach_1_status'],
            'beach_1_count': beach_data['beach_1_count'],
            'beach_2_status': beach_data['beach_2_status'], 
            'beach_2_count': beach_data['beach_2_count'],
            'beach_3_status': beach_data['beach_3_status'],
            'beach_3_count': beach_data['beach_3_count'],
            'beach_4_status': beach_data['beach_4_status'],
            'beach_4_count': beach_data['beach_4_count'],
            'overall_status': beach_data['overall_status'],
            'peak_count': beach_data['peak_count'],
            'avg_count': beach_data['avg_count'],
            'last_updated': beach_data['last_updated']
        }
        
        auth = (self.wp_username, self.wp_password)
        headers = {'Content-Type': 'application/json'}
        
        response = requests.post(wp_api_url, 
                               json={'acf': acf_data},
                               auth=auth, 
                               headers=headers)
        
        return response.status_code in [200, 201]
    
    def save_to_google_sheets(self, fwc_data, processed_data):
        """Save data to Google Sheets"""
        try:
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            creds_dict = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            client = gspread.authorize(creds)
            
            sheet = client.open_by_key(os.environ['GOOGLE_SHEET_ID'])
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Save to updated beach_status worksheet structure
            status_worksheet = sheet.worksheet('beach_status')
            
            for page_key, data in processed_data.items():
                for i in range(1, 5):
                    beach_name = data.get(f'beach_{i}_name', '')
                    if beach_name:  # Only save if beach has a name
                        row = [
                            beach_name,
                            today,
                            page_key,
                            data.get(f'beach_{i}_status', ''),
                            data.get(f'beach_{i}_count', 0),
                            data.get('overall_status', ''),
                            data.get('peak_count', 0),
                            data.get('last_updated', '')
                        ]
                        status_worksheet.append_row(row)

               # Save daily trends
            trends_worksheet = sheet.worksheet('daily_trends')
            today = datetime.now().strftime('%Y-%m-%d')
            
            for page_key, data in processed_data.items():
                for i in range(1, 5):
                    beach_name = data.get(f'beach_{i}_name', '')
                    if beach_name:
                        row = [
                            today,
                            page_key, 
                            beach_name,
                            data.get(f'beach_{i}_count', 0),
                            data.get(f'beach_{i}_status', ''),
                            '',  # sample_location
                            '',  # latitude  
                            ''   # longitude
                        ]
                        trends_worksheet.append_row(row)
            
            # Save raw FWC data
            raw_worksheet = sheet.worksheet('raw_data')
            for feature in fwc_data['features']:
                attrs = feature['attributes']
                abundance_text = attrs.get('Abundance', '')
                cell_count, _ = self.parse_abundance_number(abundance_text)
                
                row = [
                    datetime.fromtimestamp(attrs['SAMPLE_DATE'] / 1000).strftime('%Y-%m-%d'),
                    attrs.get('LOCATION', ''),
                    attrs.get('County', ''),
                    abundance_text,
                    cell_count,
                    attrs.get('LATITUDE', ''),
                    attrs.get('LONGITUDE', ''),
                    attrs.get('HAB_ID', ''),
                    datetime.fromtimestamp(attrs['ExportDate'] / 1000).strftime('%Y-%m-%d'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                raw_worksheet.append_row(row)
                    
        except Exception as e:
            print(f"Google Sheets error: {e}")
    
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
        """Main processing function"""
        print("Starting red tide processing...")
        
        # Fetch FWC data
        fwc_data = self.fetch_fwc_data()
        print(f"Fetched {len(fwc_data['features'])} samples")
        
        processed_data = {}
        
        # Process each page
        for page_key, page_config in self.beach_mapping.items():
            print(f"Processing {page_key}...")
            
            beach_data = self.process_beach_page(page_key, page_config, fwc_data)
            processed_data[page_key] = beach_data
            
            print(f"  Peak count: {beach_data['peak_count']} cells/L")
            print(f"  Overall status: {beach_data['overall_status']}")
            
            # Update WordPress
            success = self.update_wordpress_page(page_config, beach_data)
            print(f"  WordPress update: {'Success' if success else 'Failed'}")
        
        # Save to Google Sheets
        self.save_to_google_sheets(fwc_data, processed_data)
        print("Processing complete!")

if __name__ == "__main__":
    processor = RedTideProcessor()
    processor.run()
