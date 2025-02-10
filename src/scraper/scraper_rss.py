import requests
from bs4 import BeautifulSoup
import pandas as pd
import feedparser
import time
from datetime import datetime

class HespressScraper:
    def __init__(self):
        self.base_url = "https://www.hespress.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def get_topic_from_url(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            breadcrumb = soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                topic_li = breadcrumb.find_all('li')[1]
                if topic_li:
                    return topic_li.text.strip()
        except Exception as e:
            print(f"Error getting topic from {url}: {str(e)}")
        
        return 'unknown'

    def get_comments(self, article_url, article_title):
        try:
            response = requests.get(article_url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
                        
            comments_list = soup.find('ul', class_='comment-list')
            
            if not comments_list:
                return []

            comments = []
            topic = self.get_topic_from_url(article_url)
            
            comment_items = comments_list.find_all('li', class_='comment') 
            
            for i, comment in enumerate(comment_items, 1):
                try:
                    comment_text = comment.find('div', class_='comment-text')
                    comment_text = comment_text.text.strip()

                    score_element = comment.find('span', class_='comment-recat-number')
                    score = 0
                    if score_element:
                        try:
                            score = int(score_element.text.strip())
                        except ValueError:
                            print(f"Could not parse score: {score_element.text}")
                    
                    # Get comment ID from the li element's id attribute
                    comment_id = comment.get('id', '').replace('comment-', '')
                    
                    # Set created_at to the current timestamp
                    created_at = datetime.now()  # Set created_at to now

                    comment_data = {
                        'id': comment_id,
                        'comment': comment_text,
                        'topic': topic,
                        'article_title': article_title,
                        'article_url': article_url,
                        'score': score,
                        'created_at': created_at  # Use the current timestamp
                    }
                    comments.append(comment_data)
                except Exception as e:
                    print(f"Error processing comment {i}: {str(e)}")
                    continue

            return comments

        except Exception as e:
            return []

    def scrape_comments(self):
        feed = feedparser.parse('https://www.hespress.com/feed')
        
        all_comments = []
        for entry in feed.entries:
            
            cleaned_title = entry.title.replace('"', '').replace('"', '').replace('"', '').strip()
            
            comments = self.get_comments(entry.link, cleaned_title)
            if comments: 
                all_comments.extend(comments)
            time.sleep(1)

        if not all_comments: 
            return pd.DataFrame(columns=['id', 'comment', 'topic', 'article_title', 'article_url', 'score'])

        df = pd.DataFrame(all_comments)
        
        columns = ['id', 'comment', 'topic', 'article_title', 'article_url', 'score']
        if all(col in df.columns for col in columns):
            df = df[columns]
        else:
            missing_cols = [col for col in columns if col not in df.columns]
            print(f"Missing columns: {missing_cols}")
        
        return df

def main():
    scraper = HespressScraper()
    comments_df = scraper.scrape_comments()
    
    # Save to CSV file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'comments_{timestamp}.csv'
    comments_df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    print(f"Scraping completed! Data saved to {filename}")
    print(f"Total comments collected: {len(comments_df)}")

if __name__ == "__main__":
    main()
