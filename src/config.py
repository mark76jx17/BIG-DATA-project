"""
Configuration module for Urban Services Analysis.
Contains all constants, tags definitions, and category mappings.
"""

# Cities to analyze
LOCATIONS = ['Pavia, Italy', 'Cagliari, Italy']

# H3 resolution (9 = cells of ~0.1 km², ~174m per side)
H3_RESOLUTION = 9

# Minimum services threshold for "well-served" cells (15-minute city concept)
WELL_SERVED_THRESHOLD = 3

# OSM tags to download
TAGS = {
    'amenity': [
        # Health
        'hospital', 'clinic', 'doctors', 'dentist', 'pharmacy', 'veterinary',
        'nursing_home', 'social_facility', 'health_post',
        # Education
        'school', 'university', 'kindergarten', 'college', 'library',
        'music_school', 'language_school', 'driving_school', 'research_institute',
        # Food & Beverage
        'restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'ice_cream',
        'food_court', 'biergarten', 'juice_bar',
        # Retail & Services
        'marketplace', 'bank', 'atm', 'bureau_de_change',
        'post_office', 'post_box', 'parcel_locker',
        # Public Services & Safety
        'police', 'fire_station', 'townhall', 'courthouse', 'community_centre',
        'social_centre', 'public_building', 'ranger_station',
        # Culture & Entertainment
        'cinema', 'theatre', 'arts_centre', 'studio', 'events_venue',
        'nightclub', 'casino', 'gambling', 'conference_centre',
        # Religion
        'place_of_worship', 'monastery',
        # Transportation
        'bicycle_parking', 'bicycle_rental', 'car_sharing', 'charging_station',
        'ferry_terminal', 'bus_station', 'taxi',
        # Other Services
        'childcare', 'toilets', 'drinking_water', 'fountain', 'waste_basket',
        'recycling', 'waste_disposal'
    ],
    'leisure': [
        # Sports
        'sports_centre', 'pitch', 'stadium', 'swimming_pool', 'fitness_centre',
        'fitness_station', 'golf_course', 'ice_rink', 'track', 'water_park',
        # Recreation
        'park', 'playground', 'garden', 'nature_reserve', 'dog_park',
        'beach_resort', 'picnic_table', 'bandstand',
        # Entertainment
        'amusement_arcade', 'bowling_alley', 'escape_game', 'hackerspace',
        'dance'
    ],
    'shop': [
        # Food Retail
        'supermarket', 'convenience', 'bakery', 'butcher', 'greengrocer',
        'seafood', 'alcohol', 'beverages', 'cheese', 'chocolate',
        'coffee', 'confectionery', 'dairy', 'deli', 'farm',
        'frozen_food', 'health_food', 'ice_cream', 'organic', 'pasta',
        'pastry', 'spices', 'tea', 'water', 'wine',
        # General Retail
        'department_store', 'general', 'kiosk', 'mall', 'wholesale',
        # Specialized Shops (essential)
        'pharmacy', 'chemist', 'baby_goods', 'medical_supply',
        'books', 'newsagent', 'stationery',
        'hardware', 'doityourself', 'trade',
        'laundry', 'dry_cleaning',
        'hairdresser', 'beauty', 'optician', 'hearing_aids'
    ],
    'healthcare': [
        'hospital', 'clinic', 'doctor', 'dentist', 'pharmacy', 'laboratory',
        'physiotherapist', 'alternative', 'audiologist', 'blood_donation',
        'midwife', 'nurse', 'occupational_therapist', 'optometrist',
        'podiatrist', 'psychotherapist', 'rehabilitation', 'speech_therapist'
    ],
    'office': [
        'government', 'administrative', 'employment_agency', 'notary',
        'lawyer', 'accountant', 'tax_advisor', 'estate_agent'
    ],
    'tourism': [
        'information', 'hotel', 'hostel', 'guest_house', 'museum',
        'gallery', 'attraction', 'viewpoint', 'picnic_site'
    ]
}

# Category mapping for service classification
CATEGORY_MAPPING = {
    # Health
    'hospital': 'Health', 'clinic': 'Health', 'doctors': 'Health', 'doctor': 'Health',
    'dentist': 'Health', 'pharmacy': 'Health', 'veterinary': 'Health',
    'nursing_home': 'Health', 'social_facility': 'Health', 'health_post': 'Health',
    'laboratory': 'Health', 'physiotherapist': 'Health', 'alternative': 'Health',
    'audiologist': 'Health', 'blood_donation': 'Health', 'midwife': 'Health',
    'nurse': 'Health', 'occupational_therapist': 'Health', 'optometrist': 'Health',
    'podiatrist': 'Health', 'psychotherapist': 'Health', 'rehabilitation': 'Health',
    'speech_therapist': 'Health', 'chemist': 'Health', 'medical_supply': 'Health',
    'optician': 'Health', 'hearing_aids': 'Health',

    # Education
    'school': 'Education', 'university': 'Education', 'kindergarten': 'Education',
    'college': 'Education', 'library': 'Education', 'music_school': 'Education',
    'language_school': 'Education', 'driving_school': 'Education',
    'research_institute': 'Education', 'books': 'Education', 'stationery': 'Education',

    # Food & Beverage
    'restaurant': 'Food', 'cafe': 'Food', 'fast_food': 'Food', 'bar': 'Food',
    'pub': 'Food', 'ice_cream': 'Food', 'food_court': 'Food', 'biergarten': 'Food',
    'juice_bar': 'Food',

    # Food Retail
    'supermarket': 'Food Retail', 'convenience': 'Food Retail', 'bakery': 'Food Retail',
    'butcher': 'Food Retail', 'greengrocer': 'Food Retail', 'seafood': 'Food Retail',
    'alcohol': 'Food Retail', 'beverages': 'Food Retail', 'cheese': 'Food Retail',
    'chocolate': 'Food Retail', 'coffee': 'Food Retail', 'confectionery': 'Food Retail',
    'dairy': 'Food Retail', 'deli': 'Food Retail', 'farm': 'Food Retail',
    'frozen_food': 'Food Retail', 'health_food': 'Food Retail', 'organic': 'Food Retail',
    'pasta': 'Food Retail', 'pastry': 'Food Retail', 'spices': 'Food Retail',
    'tea': 'Food Retail', 'water': 'Food Retail', 'wine': 'Food Retail',

    # Retail & Shopping
    'marketplace': 'Retail', 'department_store': 'Retail', 'general': 'Retail',
    'kiosk': 'Retail', 'mall': 'Retail', 'wholesale': 'Retail',
    'baby_goods': 'Retail', 'newsagent': 'Retail',
    'hardware': 'Retail', 'doityourself': 'Retail', 'trade': 'Retail',

    # Personal Services
    'laundry': 'Services', 'dry_cleaning': 'Services', 'hairdresser': 'Services',
    'beauty': 'Services', 'childcare': 'Services',

    # Financial Services
    'bank': 'Financial', 'atm': 'Financial', 'bureau_de_change': 'Financial',
    'accountant': 'Financial', 'tax_advisor': 'Financial',

    # Public Services
    'post_office': 'Public Services', 'post_box': 'Public Services',
    'parcel_locker': 'Public Services', 'townhall': 'Public Services',
    'courthouse': 'Public Services', 'community_centre': 'Public Services',
    'social_centre': 'Public Services', 'public_building': 'Public Services',
    'government': 'Public Services', 'administrative': 'Public Services',
    'employment_agency': 'Public Services', 'ranger_station': 'Public Services',

    # Security & Emergency
    'police': 'Security', 'fire_station': 'Security',

    # Sports & Fitness
    'sports_centre': 'Sports', 'pitch': 'Sports', 'stadium': 'Sports',
    'swimming_pool': 'Sports', 'fitness_centre': 'Sports', 'fitness_station': 'Sports',
    'golf_course': 'Sports', 'ice_rink': 'Sports', 'track': 'Sports',
    'water_park': 'Sports', 'bowling_alley': 'Sports',

    # Recreation & Parks
    'park': 'Recreation', 'playground': 'Recreation', 'garden': 'Recreation',
    'nature_reserve': 'Recreation', 'dog_park': 'Recreation', 'beach_resort': 'Recreation',
    'picnic_table': 'Recreation', 'bandstand': 'Recreation', 'picnic_site': 'Recreation',

    # Culture & Entertainment
    'cinema': 'Culture', 'theatre': 'Culture', 'arts_centre': 'Culture',
    'studio': 'Culture', 'events_venue': 'Culture', 'nightclub': 'Culture',
    'casino': 'Culture', 'gambling': 'Culture', 'conference_centre': 'Culture',
    'amusement_arcade': 'Culture', 'escape_game': 'Culture', 'hackerspace': 'Culture',
    'dance': 'Culture', 'museum': 'Culture', 'gallery': 'Culture',
    'attraction': 'Culture',

    # Religion
    'place_of_worship': 'Religion', 'monastery': 'Religion',

    # Transportation
    'bicycle_parking': 'Transportation', 'bicycle_rental': 'Transportation',
    'car_sharing': 'Transportation', 'charging_station': 'Transportation',
    'ferry_terminal': 'Transportation', 'bus_station': 'Transportation',
    'taxi': 'Transportation',

    # Tourism
    'information': 'Tourism', 'hotel': 'Tourism', 'hostel': 'Tourism',
    'guest_house': 'Tourism', 'viewpoint': 'Tourism',

    # Professional Services
    'notary': 'Professional Services', 'lawyer': 'Professional Services',
    'estate_agent': 'Professional Services',

    # Public Utilities
    'toilets': 'Public Utilities', 'drinking_water': 'Public Utilities',
    'fountain': 'Public Utilities', 'waste_basket': 'Public Utilities',
    'recycling': 'Public Utilities', 'waste_disposal': 'Public Utilities'
}

# All service categories
SERVICE_CATEGORIES = [
    'Health', 'Education', 'Food', 'Food Retail', 'Retail', 'Services',
    'Financial', 'Public Services', 'Security', 'Sports', 'Recreation',
    'Culture', 'Religion', 'Transportation', 'Tourism', 'Professional Services',
    'Public Utilities'
]

# KeplerGL color configuration
KEPLER_COLOR_RANGE = {
    'name': 'Global Warming',
    'type': 'sequential',
    'category': 'Uber',
    'colors': ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']
}
