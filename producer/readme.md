# Kafka Producer

This module fetches articles from a geopolitical events API and publishes them to a Kafka topic named `geopolitics_events`.

## How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt

2. Run python kafka_producer.py 

Sample logs : 

 'seendate': '20241226T191500Z', 'socialimage': 'https://i.dailymail.co.uk/1s/2024/12/24/16/93457499-0-image-a-8_1735058121273.jpg', 'domain': 'dailymail.co.uk', 'language': 'English', 'sourcecountry': 'United Kingdom'}, {'url': 'https://www.163.com/dy/article/JK6MABF9051492T3.html', 'url_mobile': 'https://m.163.com/dy/article/JK6MABF9051492T3.html', 'title': '挑衅  买岛  和  夺河 ， 特朗普打的什么算盘 ？ 马斯克 ： 帮助  拿下 ！ 当地政府称不卖|美国|总统|华盛顿|共和党|特朗普集团|埃隆 · 马斯克|白宫新闻秘书', 'seendate': '20241224T144500Z', 'socialimage': '', 'domain': '163.com', 'language': 'Chinese', 'sourcecountry': 'China'}, {'url': 'https://www.ndtv.com/world-news/donald-trump-to-make-history-with-two-state-visits-hosted-by-britains-royal-family-7350116', 'url_mobile': 'https://www.ndtv.com/world-news/donald-trump-to-make-history-with-two-state-visits-hosted-by-britains-royal-family-7350116/amp/1', 'title': 'Donald Trump To Make History With Two State Visits Hosted By Britain Royal Family', 'seendate': '20241228T130000Z', 'socialimage': 'https://c.ndtvimg.com/2024-12/bnnnv1ao_trump-and-royals-reuters_625x300_28_December_24.jpeg', 'domain': 'ndtv.com', 'language': 'English', 'sourcecountry': 'India'}, {'url': 'https://tsn.ua/ru/politika/tramp-zanyal-vyzhidatelnuyu-poziciyu-ekspert-ocenil-kogda-budut-peregovory-s-putinym-2729538.html', 'url_mobile': 'https://tsn.ua/ru/amp/politika/tramp-zanyal-vyzhidatelnuyu-poziciyu-ekspert-ocenil-kogda-budut-peregovory-s-putinym-2729538.html', 'title': 'Переговоры Трампа с Путиным маловероятны до февраля 2025 года – эксперт Политика', 'seendate': '20241223T173000Z', 'socialimage': 'https://img.tsn.ua/cached/402/tsn-d1c7980c2793b1401d3d1b4f99d2baf6