"""
Synthetic Data Generator for Application Testing
=================================================

This script generates synthetic CSV data files for testing purposes, creating both
full historical datasets and incremental update files.

Generated Files:
    - Customer File: Basic customer information
    - Product File: Electronics product catalog with pricing
    - Transaction File: Customer purchase transactions
    - Location File: Store location information

Author: Data Engineering Team
Version: 1.0.0
Date: 2025-12-22
"""

import csv
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict


# ============================================================================
# CONFIGURATION PARAMETERS
# ============================================================================

@dataclass
class DataConfig:
    """Configuration parameters for data generation."""
    
    # Core data volumes
    num_customers: int = 2000
    num_products: int = 150
    num_locations: int = 50
    total_transactions: int = 70000
    
    # Transaction constraints per customer
    min_transactions_per_customer: int = 2
    max_transactions_per_customer: int = 10
    
    # Date ranges
    historical_start_date: str = "2022-01-01"
    historical_end_date: str = "2024-12-15"
    incremental_start_date: str = "2024-12-16"
    incremental_end_date: str = "2024-12-22"
    
    # Incremental data percentages (% of full data)
    incremental_customer_pct: float = 0.05  # 5% new customers
    incremental_transaction_pct: float = 0.03  # 3% of total transactions
    
    # Output directories
    output_dir_full: str = "data_full"
    output_dir_incremental: str = "data_incremental"
    
    # File naming
    customer_file: str = "customers.csv"
    product_file: str = "products.csv"
    transaction_file: str = "transactions.csv"
    location_file: str = "locations.csv"


# ============================================================================
# DATA GENERATION FUNCTIONS
# ============================================================================

class SyntheticDataGenerator:
    """Main class for generating synthetic data files."""
    
    def __init__(self, config: DataConfig):
        """
        Initialize the data generator.
        
        Args:
            config: DataConfig object with generation parameters
        """
        self.config = config
        random.seed(42)  # For reproducible results
        
    def generate_customers(self, num_customers: int, start_id: int = 1) -> List[Dict]:
        """
        Generate customer records.
        
        Args:
            num_customers: Number of customers to generate
            start_id: Starting customer ID
            
        Returns:
            List of customer dictionaries
        """
        first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", 
            "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
            "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen", "Charles",
            "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret",
            "Mark", "Sandra", "Donald", "Ashley", "Steven", "Kimberly", "Andrew",
            "Emily", "Paul", "Donna", "Joshua", "Michelle", "Kenneth", "Carol",
            "Kevin", "Amanda", "Brian", "Melissa", "George", "Deborah", "Timothy",
            "Stephanie", "Ronald", "Dorothy", "Edward", "Rebecca", "Jason", "Sharon"
        ]
        
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
            "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
            "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
            "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
            "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
            "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
            "Carter", "Roberts"
        ]
        
        customers = []
        for i in range(num_customers):
            customer_id = start_id + i
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            
            customer = {
                'customer_id': customer_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': f"{first_name.lower()}.{last_name.lower()}{customer_id}@email.com",
                'phone': f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
                'city': random.choice([
                    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
                    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
                    "San Francisco", "Indianapolis", "Seattle", "Denver", "Boston"
                ]),
                'state': random.choice([
                    "NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "NC", "WA",
                    "CO", "MA", "GA", "MI", "VA", "NJ", "MD", "OR", "MN", "NV"
                ]),
                'zip_code': f"{random.randint(10000, 99999)}",
                'registration_date': self._random_date(
                    self.config.historical_start_date,
                    self.config.historical_end_date
                )
            }
            customers.append(customer)
            
        return customers
    
    def generate_products(self) -> List[Dict]:
        """
        Generate electronics product records.
        
        Returns:
            List of product dictionaries
        """
        # Electronics product categories and items
        products_data = [
            # Laptops
            ("Laptop", "UltraBook Pro 15", 1299.99),
            ("Laptop", "Business Notebook 14", 899.99),
            ("Laptop", "Gaming Laptop X1", 1899.99),
            ("Laptop", "Student Laptop Lite", 599.99),
            ("Laptop", "Creator Workstation 17", 2499.99),
            ("Laptop", "Ultralight Travel Book", 1099.99),
            ("Laptop", "Budget Essential Laptop", 449.99),
            ("Laptop", "Premium MacBook Air", 1399.99),
            ("Laptop", "Gaming Beast Pro", 2299.99),
            ("Laptop", "Chromebook Basic", 329.99),
            
            # Smartphones
            ("Smartphone", "Galaxy S Ultra", 1199.99),
            ("Smartphone", "iPhone Pro Max", 1299.99),
            ("Smartphone", "Pixel Premium", 899.99),
            ("Smartphone", "Budget Android Phone", 299.99),
            ("Smartphone", "OnePlus Flagship", 799.99),
            ("Smartphone", "Moto G Series", 249.99),
            ("Smartphone", "iPhone Standard", 799.99),
            ("Smartphone", "Galaxy A Series", 449.99),
            ("Smartphone", "Pixel Budget", 499.99),
            ("Smartphone", "Xiaomi Value Phone", 349.99),
            
            # Tablets
            ("Tablet", "iPad Pro 12.9", 1099.99),
            ("Tablet", "Galaxy Tab S8", 849.99),
            ("Tablet", "iPad Air", 599.99),
            ("Tablet", "Fire Tablet HD", 149.99),
            ("Tablet", "Surface Pro", 1299.99),
            ("Tablet", "Budget Android Tablet", 199.99),
            ("Tablet", "iPad Mini", 499.99),
            ("Tablet", "Galaxy Tab A", 329.99),
            
            # Headphones
            ("Headphones", "AirPods Pro", 249.99),
            ("Headphones", "Sony WH-1000XM5", 399.99),
            ("Headphones", "Bose QuietComfort", 349.99),
            ("Headphones", "Budget Earbuds", 49.99),
            ("Headphones", "Gaming Headset RGB", 129.99),
            ("Headphones", "Studio Monitor Headphones", 299.99),
            ("Headphones", "Wireless Earbuds Basic", 79.99),
            ("Headphones", "Sports Earbuds", 99.99),
            
            # Smart Watches
            ("Smart Watch", "Apple Watch Ultra", 799.99),
            ("Smart Watch", "Galaxy Watch Pro", 449.99),
            ("Smart Watch", "Fitbit Premium", 299.99),
            ("Smart Watch", "Budget Fitness Tracker", 79.99),
            ("Smart Watch", "Garmin Sports Watch", 549.99),
            ("Smart Watch", "Amazfit Smart Watch", 199.99),
            
            # Monitors
            ("Monitor", "4K UHD Monitor 27", 449.99),
            ("Monitor", "Gaming Monitor 144Hz", 599.99),
            ("Monitor", "Ultrawide 34", 799.99),
            ("Monitor", "Budget 1080p 24", 179.99),
            ("Monitor", "Professional 4K 32", 899.99),
            ("Monitor", "Curved Gaming 27", 499.99),
            
            # Keyboards & Mice
            ("Keyboard", "Mechanical Gaming Keyboard", 149.99),
            ("Keyboard", "Wireless Keyboard Combo", 59.99),
            ("Keyboard", "Ergonomic Keyboard", 89.99),
            ("Keyboard", "Budget Keyboard", 24.99),
            ("Mouse", "Gaming Mouse RGB", 79.99),
            ("Mouse", "Wireless Mouse", 39.99),
            ("Mouse", "Ergonomic Mouse", 49.99),
            ("Mouse", "Budget Mouse", 14.99),
            
            # Speakers
            ("Speaker", "Bluetooth Speaker Portable", 99.99),
            ("Speaker", "Smart Speaker", 129.99),
            ("Speaker", "Desktop Speakers 2.1", 79.99),
            ("Speaker", "Soundbar Premium", 299.99),
            ("Speaker", "Budget Bluetooth Speaker", 39.99),
            
            # Cameras
            ("Camera", "DSLR Camera Professional", 1499.99),
            ("Camera", "Mirrorless Camera", 1199.99),
            ("Camera", "Action Camera 4K", 349.99),
            ("Camera", "Webcam HD Pro", 129.99),
            ("Camera", "Security Camera Indoor", 79.99),
            ("Camera", "Point and Shoot Camera", 449.99),
            
            # Storage
            ("Storage", "External SSD 1TB", 149.99),
            ("Storage", "External HDD 4TB", 119.99),
            ("Storage", "USB Flash Drive 128GB", 24.99),
            ("Storage", "NAS 2-Bay System", 299.99),
            ("Storage", "SD Card 256GB", 39.99),
            
            # Networking
            ("Router", "WiFi 6 Router Premium", 249.99),
            ("Router", "Mesh WiFi System 3-Pack", 399.99),
            ("Router", "Budget WiFi Router", 49.99),
            ("Networking", "Ethernet Switch 8-Port", 79.99),
            ("Networking", "WiFi Extender", 59.99),
            
            # Chargers & Cables
            ("Accessory", "Fast Charger 65W", 49.99),
            ("Accessory", "Wireless Charging Pad", 29.99),
            ("Accessory", "USB-C Cable 6ft", 19.99),
            ("Accessory", "Power Bank 20000mAh", 59.99),
            ("Accessory", "Multi-Port USB Hub", 39.99),
            
            # Cases & Protection
            ("Accessory", "Laptop Sleeve 15", 29.99),
            ("Accessory", "Phone Case Premium", 39.99),
            ("Accessory", "Screen Protector", 14.99),
            ("Accessory", "Tablet Stand", 24.99),
            
            # Gaming
            ("Gaming", "Gaming Controller Wireless", 69.99),
            ("Gaming", "VR Headset", 399.99),
            ("Gaming", "Gaming Chair RGB", 299.99),
            ("Gaming", "Streaming Microphone", 129.99),
            
            # Smart Home
            ("Smart Home", "Smart Light Bulbs 4-Pack", 49.99),
            ("Smart Home", "Smart Plug 2-Pack", 29.99),
            ("Smart Home", "Video Doorbell", 179.99),
            ("Smart Home", "Smart Thermostat", 249.99),
            ("Smart Home", "Security Camera Outdoor", 149.99),
        ]
        
        products = []
        for i, (category, name, price) in enumerate(products_data, start=1):
            product = {
                'product_id': i,
                'product_name': name,
                'category': category,
                'unit_price': f"{price:.2f}",
                'stock_quantity': random.randint(50, 500),
                'manufacturer': random.choice([
                    "Apple", "Samsung", "Sony", "LG", "Dell", "HP", "Lenovo",
                    "Microsoft", "Google", "Asus", "Acer", "Logitech", "Razer",
                    "Corsair", "Bose", "JBL", "Anker", "Belkin", "TP-Link", "Netgear"
                ])
            }
            products.append(product)
            
        return products
    
    def generate_locations(self) -> List[Dict]:
        """
        Generate store location records.
        
        Returns:
            List of location dictionaries
        """
        store_cities = [
            ("New York", "NY", "Manhattan"),
            ("New York", "NY", "Brooklyn"),
            ("Los Angeles", "CA", "Downtown LA"),
            ("Los Angeles", "CA", "Santa Monica"),
            ("Chicago", "IL", "Loop"),
            ("Chicago", "IL", "River North"),
            ("Houston", "TX", "Downtown"),
            ("Houston", "TX", "Galleria"),
            ("Phoenix", "AZ", "Scottsdale"),
            ("Phoenix", "AZ", "Tempe"),
            ("Philadelphia", "PA", "Center City"),
            ("San Antonio", "TX", "River Walk"),
            ("San Diego", "CA", "Gaslamp"),
            ("Dallas", "TX", "Uptown"),
            ("San Jose", "CA", "Downtown"),
            ("Austin", "TX", "Downtown"),
            ("Jacksonville", "FL", "Southbank"),
            ("Fort Worth", "TX", "Sundance Square"),
            ("Columbus", "OH", "Short North"),
            ("Charlotte", "NC", "Uptown"),
            ("San Francisco", "CA", "Union Square"),
            ("Indianapolis", "IN", "Downtown"),
            ("Seattle", "WA", "Downtown"),
            ("Denver", "CO", "LoDo"),
            ("Boston", "MA", "Back Bay"),
            ("Portland", "OR", "Pearl District"),
            ("Las Vegas", "NV", "The Strip"),
            ("Detroit", "MI", "Downtown"),
            ("Memphis", "TN", "Beale Street"),
            ("Nashville", "TN", "Downtown"),
            ("Baltimore", "MD", "Inner Harbor"),
            ("Milwaukee", "WI", "Third Ward"),
            ("Albuquerque", "NM", "Old Town"),
            ("Tucson", "AZ", "Downtown"),
            ("Fresno", "CA", "Tower District"),
            ("Sacramento", "CA", "Midtown"),
            ("Kansas City", "MO", "Plaza"),
            ("Mesa", "AZ", "Downtown"),
            ("Atlanta", "GA", "Midtown"),
            ("Omaha", "NE", "Old Market"),
            ("Miami", "FL", "Brickell"),
            ("Oakland", "CA", "Jack London Square"),
            ("Tulsa", "OK", "Blue Dome"),
            ("Minneapolis", "MN", "Downtown"),
            ("Cleveland", "OH", "Downtown"),
            ("Wichita", "KS", "Old Town"),
            ("Arlington", "TX", "Entertainment District"),
            ("Tampa", "FL", "Channelside"),
            ("New Orleans", "LA", "French Quarter"),
            ("Bakersfield", "CA", "Downtown"),
        ]
        
        locations = []
        for i in range(self.config.num_locations):
            city, state, district = store_cities[i % len(store_cities)]
            location = {
                'location_id': i + 1,
                'store_name': f"TechStore {district}",
                'street_address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Market', 'Oak', 'Pine', 'Maple', 'Cedar'])} St",
                'city': city,
                'state': state,
                'zip_code': f"{random.randint(10000, 99999)}",
                'store_size_sqft': random.choice([5000, 7500, 10000, 12500, 15000]),
                'opening_date': self._random_date("2015-01-01", "2023-12-31")
            }
            locations.append(location)
            
        return locations
    
    def generate_transactions(
        self,
        customers: List[Dict],
        products: List[Dict],
        locations: List[Dict],
        start_date: str,
        end_date: str,
        num_transactions: int = None
    ) -> List[Dict]:
        """
        Generate transaction records.
        
        Args:
            customers: List of customer dictionaries
            products: List of product dictionaries
            locations: List of location dictionaries
            start_date: Start date for transactions (YYYY-MM-DD)
            end_date: End date for transactions (YYYY-MM-DD)
            num_transactions: Number of transactions to generate (if None, uses config)
            
        Returns:
            List of transaction dictionaries
        """
        if num_transactions is None:
            num_transactions = self.config.total_transactions
            
        transactions = []
        transaction_id = 1
        
        # Ensure each customer has between min and max transactions
        for customer in customers:
            customer_id = customer['customer_id']
            num_cust_transactions = random.randint(
                self.config.min_transactions_per_customer,
                self.config.max_transactions_per_customer
            )
            
            for _ in range(num_cust_transactions):
                product = random.choice(products)
                location = random.choice(locations)
                quantity = random.randint(1, 3)
                unit_price = float(product['unit_price'])
                
                transaction = {
                    'transaction_id': transaction_id,
                    'customer_id': customer_id,
                    'product_id': product['product_id'],
                    'location_id': location['location_id'],
                    'transaction_date': self._random_date(start_date, end_date),
                    'quantity': quantity,
                    'unit_price': f"{unit_price:.2f}",
                    'total_amount': f"{(unit_price * quantity):.2f}",
                    'payment_method': random.choice([
                        "Credit Card", "Debit Card", "PayPal", "Cash", "Gift Card"
                    ])
                }
                transactions.append(transaction)
                transaction_id += 1
                
        # Sort transactions by date
        transactions.sort(key=lambda x: x['transaction_date'])
        
        # Reassign transaction IDs sequentially
        for i, transaction in enumerate(transactions, start=1):
            transaction['transaction_id'] = i
            
        return transactions
    
    def _random_date(self, start_date: str, end_date: str) -> str:
        """
        Generate a random date between start_date and end_date.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Random date in YYYY-MM-DD format
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        delta = end - start
        random_days = random.randint(0, delta.days)
        random_date = start + timedelta(days=random_days)
        
        return random_date.strftime("%Y-%m-%d")
    
    def write_csv(self, data: List[Dict], filepath: str) -> None:
        """
        Write data to CSV file.
        
        Args:
            data: List of dictionaries to write
            filepath: Output file path
        """
        if not data:
            print(f"Warning: No data to write to {filepath}")
            return
            
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            
        print(f"✓ Created: {filepath} ({len(data):,} records)")
    
    def generate_full_dataset(self) -> None:
        """Generate full historical dataset."""
        print("\n" + "="*70)
        print("GENERATING FULL HISTORICAL DATASET")
        print("="*70 + "\n")
        
        # Generate all data
        print("Generating customers...")
        customers = self.generate_customers(self.config.num_customers)
        
        print("Generating products...")
        products = self.generate_products()
        
        print("Generating locations...")
        locations = self.generate_locations()
        
        print("Generating transactions...")
        transactions = self.generate_transactions(
            customers,
            products,
            locations,
            self.config.historical_start_date,
            self.config.historical_end_date
        )
        
        # Write files
        print("\nWriting files...")
        output_dir = self.config.output_dir_full
        
        self.write_csv(
            customers,
            os.path.join(output_dir, self.config.customer_file)
        )
        self.write_csv(
            products,
            os.path.join(output_dir, self.config.product_file)
        )
        self.write_csv(
            locations,
            os.path.join(output_dir, self.config.location_file)
        )
        self.write_csv(
            transactions,
            os.path.join(output_dir, self.config.transaction_file)
        )
        
        print(f"\n✓ Full dataset generation complete!")
        print(f"  Output directory: {output_dir}/")
        
        # Store data for incremental generation
        self.full_customers = customers
        self.full_products = products
        self.full_locations = locations
        self.full_transactions = transactions
    
    def generate_incremental_dataset(self) -> None:
        """Generate incremental dataset with new records."""
        print("\n" + "="*70)
        print("GENERATING INCREMENTAL DATASET")
        print("="*70 + "\n")
        
        # Calculate incremental sizes
        num_new_customers = int(self.config.num_customers * self.config.incremental_customer_pct)
        num_new_transactions = int(self.config.total_transactions * self.config.incremental_transaction_pct)
        
        print(f"New customers: {num_new_customers:,}")
        print(f"New transactions: {num_new_transactions:,}\n")
        
        # Generate new customers
        print("Generating new customers...")
        start_customer_id = max(c['customer_id'] for c in self.full_customers) + 1
        new_customers = self.generate_customers(num_new_customers, start_customer_id)
        
        # Combine for transaction generation
        all_customers = self.full_customers + new_customers
        
        # Generate new transactions
        print("Generating new transactions...")
        # Calculate starting transaction ID
        start_transaction_id = max(t['transaction_id'] for t in self.full_transactions) + 1
        
        # Generate new transactions with proper distribution
        new_transactions = []
        transaction_id = start_transaction_id
        
        # Ensure new customers get some transactions
        customers_for_transactions = new_customers + random.sample(
            self.full_customers,
            min(len(self.full_customers), num_new_transactions // 2)
        )
        
        for customer in customers_for_transactions[:num_new_transactions]:
            product = random.choice(self.full_products)
            location = random.choice(self.full_locations)
            quantity = random.randint(1, 3)
            unit_price = float(product['unit_price'])
            
            transaction = {
                'transaction_id': transaction_id,
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'location_id': location['location_id'],
                'transaction_date': self._random_date(
                    self.config.incremental_start_date,
                    self.config.incremental_end_date
                ),
                'quantity': quantity,
                'unit_price': f"{unit_price:.2f}",
                'total_amount': f"{(unit_price * quantity):.2f}",
                'payment_method': random.choice([
                    "Credit Card", "Debit Card", "PayPal", "Cash", "Gift Card"
                ])
            }
            new_transactions.append(transaction)
            transaction_id += 1
            
            if len(new_transactions) >= num_new_transactions:
                break
        
        # Sort by date
        new_transactions.sort(key=lambda x: x['transaction_date'])
        
        # Write incremental files
        print("\nWriting incremental files...")
        output_dir = self.config.output_dir_incremental
        
        self.write_csv(
            new_customers,
            os.path.join(output_dir, self.config.customer_file)
        )
        # Products and locations remain unchanged in incremental
        self.write_csv(
            [],  # No new products in incremental
            os.path.join(output_dir, self.config.product_file)
        )
        self.write_csv(
            [],  # No new locations in incremental
            os.path.join(output_dir, self.config.location_file)
        )
        self.write_csv(
            new_transactions,
            os.path.join(output_dir, self.config.transaction_file)
        )
        
        print(f"\n✓ Incremental dataset generation complete!")
        print(f"  Output directory: {output_dir}/")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    print("\n" + "="*70)
    print(" SYNTHETIC DATA GENERATOR FOR APPLICATION TESTING")
    print("="*70)
    
    # Initialize configuration
    config = DataConfig()
    
    # Display configuration
    print("\nConfiguration:")
    print(f"  Customers: {config.num_customers:,}")
    print(f"  Products: {config.num_products}")
    print(f"  Locations: {config.num_locations}")
    print(f"  Total Transactions: {config.total_transactions:,}")
    print(f"  Transactions per Customer: {config.min_transactions_per_customer}-{config.max_transactions_per_customer}")
    print(f"  Historical Period: {config.historical_start_date} to {config.historical_end_date}")
    print(f"  Incremental Period: {config.incremental_start_date} to {config.incremental_end_date}")
    
    # Initialize generator
    generator = SyntheticDataGenerator(config)
    
    # Generate datasets
    generator.generate_full_dataset()
    generator.generate_incremental_dataset()
    
    # Summary
    print("\n" + "="*70)
    print(" GENERATION COMPLETE")
    print("="*70)
    print(f"\nFull dataset location: {config.output_dir_full}/")
    print(f"Incremental dataset location: {config.output_dir_incremental}/")
    print("\nFiles generated:")
    print(f"  - {config.customer_file}")
    print(f"  - {config.product_file}")
    print(f"  - {config.transaction_file}")
    print(f"  - {config.location_file}")
    print("\n✓ All files generated successfully!\n")


if __name__ == "__main__":
    main()
