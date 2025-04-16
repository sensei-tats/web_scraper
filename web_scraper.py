#!/usr/bin/env python3
"""
Web Scraper for Vacancy Mail Zimbabwe
This script scrapes job listings from https://vacancymail.co.zw/jobs/,
extracts relevant information, and saves it to a CSV file.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import schedule
import time
from datetime import datetime
import os
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

class VacancyMailScraper:
    """A web scraper for VacancyMail Zimbabwe job listings."""
    
    def __init__(self, base_url="https://vacancymail.co.zw/jobs/", output_file="scraped_data.csv"):
        """Initialize the scraper with the target URL and output file name."""
        self.base_url = base_url
        self.output_file = output_file
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def fetch_page(self, url):
        """Fetch the HTML content of a webpage."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None
    
    def parse_job_listings(self, html_content):
        """Extract job listings from the HTML content based on observed structure."""
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        try:
            job_listings = []
            
            # Find all job links - based on the observed data structure
            job_links = soup.find_all('a', href=True)
            job_related_links = [a for a in job_links if any(keyword in a.get_text().lower() or keyword in a['href'].lower() 
                                                          for keyword in ['job', 'vacancy', 'position', 'career'])]
            
            # Get only unique job titles
            seen_titles = set()
            unique_jobs = []
            
            for link in job_related_links:
                title = link.get_text().strip()
                if title and title not in seen_titles and not title.lower().startswith('next'):
                    seen_titles.add(title)
                    unique_jobs.append(link)
            
            # Process only the first 10 unique jobs
            for link in unique_jobs[:10]:
                job_data = {}
                job_data['title'] = link.get_text().strip()
                job_data['url'] = f"{self.base_url}{link['href'].lstrip('/')}" if not link['href'].startswith('http') else link['href']
                
                # Go to the job page to get more details
                job_details = self.extract_job_details(job_data['url'])
                job_data.update(job_details)
                
                job_listings.append(job_data)
                logging.info(f"Scraped job: {job_data['title']}")
            
            return job_listings
            
        except Exception as e:
            logging.error(f"Error parsing job listings: {e}")
            return []
    
    def extract_job_details(self, job_url):
        """Extract job details from an individual job page."""
        details = {
            'company': "N/A",
            'location': "Harare, Zimbabwe",  # Default location if not found
            'expiry_date': "N/A",
            'description': "N/A"
        }
        
        try:
            html_content = self.fetch_page(job_url)
            if not html_content:
                return details
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract company name - based on observed patterns
            company_element = soup.find('h3') or soup.find(class_='company')
            if company_element:
                details['company'] = company_element.get_text().strip()
            
            # Extract description
            description_div = soup.find(class_='job-description') or soup.find(class_='content')
            if description_div:
                # Get the text and limit its length
                description = description_div.get_text(strip=True, separator=' ')
                if len(description) > 300:
                    description = description[:297] + "..."
                details['description'] = description
            
            # Look for dates
            date_pattern = r'(?:Expiry|Closing|Deadline|Due)(?:\s+Date)?:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+[A-Za-z]+\s+\d{2,4})'
            text = soup.get_text()
            date_match = re.search(date_pattern, text, re.IGNORECASE)
            if date_match:
                details['expiry_date'] = date_match.group(1).strip()
            
            # Look for location indicators
            location_pattern = r'(?:Location|Place|City|Town|Based in|Position in)(?:\s*:)?\s*([A-Za-z\s,]+)(?:\.|,|\n)'
            location_match = re.search(location_pattern, text, re.IGNORECASE)
            if location_match:
                details['location'] = location_match.group(1).strip()
            
            return details
                
        except Exception as e:
            logging.error(f"Error extracting job details from {job_url}: {e}")
            return details
    
    def clean_data(self, job_listings):
        """Clean and format the scraped data."""
        if not job_listings:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(job_listings)
        
        # Remove duplicate entries based on job title
        df.drop_duplicates(subset=['title'], keep='first', inplace=True)
        
        # Standardize date format if possible
        def standardize_date(date_str):
            if date_str == "N/A":
                return date_str
            try:
                # Try to parse the date and convert to standard format
                date_formats = ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%B %d, %Y", "%d %B %Y", "%d %b %Y"]
                for fmt in date_formats:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        return date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
                return date_str  # Return original if parsing fails
            except Exception:
                return date_str
                
        df['expiry_date'] = df['expiry_date'].apply(standardize_date)
        
        # Add scraping timestamp
        df['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Ensure all expected columns exist
        expected_columns = ['title', 'url', 'company', 'location', 'expiry_date', 'description', 'scraped_at']
        for col in expected_columns:
            if col not in df.columns:
                df[col] = "N/A"
        
        # Select and order the expected columns
        df = df[expected_columns]
        
        return df
    
    def save_to_csv(self, df):
        """Save the DataFrame to a CSV file."""
        try:
            if df.empty:
                logging.warning("No data to save.")
                return False
                
            # Save to CSV (overwriting any existing file)
            df.to_csv(self.output_file, index=False)
            
            logging.info(f"Data saved to {self.output_file}")
            return True
        except Exception as e:
            logging.error(f"Error saving data to CSV: {e}")
            return False
    
    def run(self):
        """Run the complete scraping process."""
        logging.info("Starting scraping process...")
        
        try:
            # Fetch the main page
            html_content = self.fetch_page(self.base_url)
            if not html_content:
                logging.error("Failed to fetch the main page. Aborting.")
                return False
            
            # Parse job listings
            job_listings = self.parse_job_listings(html_content)
            if not job_listings:
                logging.warning("No job listings found.")
                return False
            
            # Clean the data
            df = self.clean_data(job_listings)
            
            # Save to CSV
            success = self.save_to_csv(df)
            
            logging.info(f"Scraping completed. Found {len(job_listings)} jobs.")
            return success
            
        except Exception as e:
            logging.error(f"Unexpected error during scraping process: {e}")
            return False


def schedule_scraping(interval='daily'):
    """Schedule the scraping task at regular intervals."""
    scraper = VacancyMailScraper()
    
    if interval == 'hourly':
        schedule.every().hour.do(scraper.run)
        logging.info("Scraping scheduled to run every hour.")
    elif interval == 'daily':
        schedule.every().day.at("09:00").do(scraper.run)
        logging.info("Scraping scheduled to run daily at 09:00.")
    elif interval == 'weekly':
        schedule.every().monday.at("09:00").do(scraper.run)
        logging.info("Scraping scheduled to run weekly on Monday at 09:00.")
    else:
        logging.error(f"Invalid interval: {interval}")
        return False
    
    # Run once immediately
    scraper.run()
    
    # Keep the script running to execute scheduled tasks
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute if there are pending tasks


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Web scraper for VacancyMail Zimbabwe job listings.")
    parser.add_argument("--schedule", "-s", choices=['none', 'hourly', 'daily', 'weekly'], 
                        default='none', help="Schedule scraping at regular intervals.")
    parser.add_argument("--output", "-o", default="scraped_data.csv", 
                        help="Output file path (default: scraped_data.csv)")
    
    args = parser.parse_args()
    
    if args.schedule != 'none':
        try:
            schedule_scraping(args.schedule)
        except KeyboardInterrupt:
            logging.info("Scheduling stopped by user.")
    else:
        # Run once without scheduling
        scraper = VacancyMailScraper(output_file=args.output)
        success = scraper.run()
        if success:
            logging.info("Scraping completed successfully.")
        else:
            logging.error("Scraping process failed.")