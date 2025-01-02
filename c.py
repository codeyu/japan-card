import requests
from bs4 import BeautifulSoup
import json
import time
import random
import asyncio
import aiohttp
from typing import List, Dict
import math
import sys
import os
import chardet
from datetime import datetime

def load_existing_cards(filename: str) -> Dict[str, Dict]:
    """加载现有的卡片数据，返回以卡片名称为键的字典"""
    if not os.path.exists(filename):
        return {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            cards = json.load(f)
            return {card['name']: card for card in cards if 'name' in card}
    except Exception as e:
        print(f"Error loading existing cards: {e}")
        return {}

def merge_cards(existing_cards: Dict[str, Dict], new_cards: List[Dict]) -> List[Dict]:
    """合并现有卡片和新卡片数据"""
    merged = existing_cards.copy()
    current_time = datetime.now().isoformat()
    
    for card in new_cards:
        if 'name' not in card:
            continue
            
        if card['name'] in merged:
            # 更新现有卡片信息
            merged[card['name']].update(card)
            # 更新更新时间
            merged[card['name']]['updated_at'] = current_time
        else:
            # 添加新卡片，设置创建时间和更新时间
            card['created_at'] = current_time
            card['updated_at'] = current_time
            merged[card['name']] = card
    
    return list(merged.values())

async def get_card_info(session: aiohttp.ClientSession, url: str) -> List[Dict]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        async with session.get(url, headers=headers) as response:
            try:
                # 首先尝试自动编码
                html = await response.text()
            except UnicodeDecodeError:
                print(f"Encoding error on {url}, retrying with cp932...")
                # 如果失败，使用 cp932 编码重试
                content = await response.read()
                html = content.decode('cp932', errors='ignore')
            
            soup = BeautifulSoup(html, 'html.parser')
            
            cards = []
            card_items = soup.find_all('li', class_='p-planSearchList_item')
            
            for item in card_items:
                card = {}
                
                # 获取卡片图片链接
                card_img = item.find('div', class_='main-card')
                if card_img and card_img.find('img'):
                    card['image_url'] = card_img.find('img')['src']
                
                # 获取卡片规格信息
                specs = item.find_all('li', class_='p-itemBox_data_spec_list')
                for spec in specs:
                    head = spec.find('div', class_='p-itemBox_data_spec_head').text.strip()
                    detail = spec.find('div', class_='p-itemBox_data_spec_detail')
                    
                    if head == '国際ブランド':
                        # 修改这里：获取品牌图片和名称
                        brands = []
                        brand_items = detail.find_all('li', class_='p-itemBox_data_brand_item')
                        for brand_item in brand_items:
                            brand_img = brand_item.find('img')
                            if brand_img:
                                brands.append({
                                    'image_url': brand_img['src'],
                                    'name': brand_img['alt']
                                })
                        card['brands'] = brands
                    elif head == '年会費':
                        card['annual_fee'] = detail.text.strip()
                    elif head == 'ポイント還元率':
                        card['point_rate'] = detail.text.strip()
                
                # 获取卡片名称和详情链接
                card_info = item.find('div', class_='card-spec-blk')
                if card_info:
                    name_link = card_info.find('a', class_='p-planSearchList_name_link')
                    if name_link:
                        card['name'] = name_link.text.strip()
                        card['detail_url'] = 'https://kakaku.com' + name_link['href']
                
                # 获取排名
                rank = item.find('span', class_='rank-box')
                if rank:
                    rank_num = rank.find('span')
                    if rank_num:
                        card['rank'] = rank_num.text.strip()
                
                # 获取卡片介绍
                catch_ttl = item.find('p', class_='p-itemBox_catch_ttl')
                if catch_ttl:
                    card['introduction_title'] = catch_ttl.text.strip()
                
                catch_txt = item.find('p', class_='p-itemBox_catch_txt')
                if catch_txt:
                    card['introduction_text'] = catch_txt.text.strip()
                
                # 获取卡片特征
                features = []
                recm_items = item.find_all('li', class_='p-itemBox_recm_item')
                for recm in recm_items:
                    features.append(recm.text.strip())
                card['features'] = features
                
                cards.append(card)
            
            # 随机延迟2-5秒
            await asyncio.sleep(random.uniform(2, 5))
            return cards
    
    except Exception as e:
        print(f"Error fetching page {url}: {e}")
        return []

async def process_page_group(session: aiohttp.ClientSession, pages: List[int]) -> List[Dict]:
    base_url = "https://kakaku.com/card/list/?cc_page="
    tasks = []
    
    for page in pages:
        url = base_url + str(page)
        print(f"Adding page {page} to queue...")
        tasks.append(get_card_info(session, url))
    
    results = await asyncio.gather(*tasks)
    return [card for page_cards in results for card in page_cards]

async def main():
    all_cards = []
    
    # 处理命令行参数
    if len(sys.argv) > 1:
        try:
            pages = [int(p.strip()) for p in sys.argv[1].split(',')]
            print(f"Will scrape specific pages: {pages}")
            output_file = f'credit_cards_pages_{sys.argv[1].replace(",", "_")}.json'
            
            async with aiohttp.ClientSession() as session:
                print(f"\nProcessing pages {pages}...")
                group_cards = await process_page_group(session, pages)
                all_cards.extend(group_cards)
                
                print(f"Completed pages {pages}. Cards collected: {len(all_cards)}")
        
        except ValueError as e:
            print("Error: Please provide valid page numbers separated by commas")
            print("Example: python c.py 47,48")
            return
    
    else:
        # 如果没有参数，爬取所有页面
        output_file = 'data/credit_cards_all.json'
        total_pages = 114
        group_size = 5
        num_groups = math.ceil(total_pages / group_size)
        
        async with aiohttp.ClientSession() as session:
            for group in range(num_groups):
                start_page = group * group_size + 1
                end_page = min((group + 1) * group_size, total_pages)
                pages = list(range(start_page, end_page + 1))
                
                print(f"\nProcessing pages {start_page} to {end_page}...")
                group_cards = await process_page_group(session, pages)
                all_cards.extend(group_cards)
                
                print(f"Completed pages {start_page} to {end_page}. Cards collected so far: {len(all_cards)}")
    
    # 加载现有数据并合并
    existing_cards = load_existing_cards(output_file)
    if existing_cards:
        print(f"\nFound {len(existing_cards)} existing cards in {output_file}")
        merged_cards = merge_cards(existing_cards, all_cards)
        print(f"After merging: {len(merged_cards)} total cards")
    else:
        print(f"\nNo existing cards found in {output_file}")
        # 如果是全新的数据，为所有卡片添加时间戳
        current_time = datetime.now().isoformat()
        for card in all_cards:
            card['created_at'] = current_time
            card['updated_at'] = current_time
        merged_cards = all_cards
    
    # 保存到JSON文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_cards, f, ensure_ascii=False, indent=2)
    
    print(f"\nTotal cards saved: {len(merged_cards)}")
    print(f"Data saved to: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
