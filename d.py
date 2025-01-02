import json
import os
import aiohttp
import asyncio
from urllib.parse import urlparse
import glob

async def download_image(session: aiohttp.ClientSession, url: str, save_path: str) -> bool:
    """下载单个图片"""
    try:
        # 确保目标目录存在
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        async with session.get(url) as response:
            if response.status == 200:
                with open(save_path, 'wb') as f:
                    f.write(await response.read())
                print(f"Downloaded: {url}")
                return True
            else:
                print(f"Failed to download {url}, status: {response.status}")
                return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def get_filename_from_url(url: str) -> str:
    """从URL中提取文件名"""
    return os.path.basename(urlparse(url).path)

def load_existing_images(directory: str) -> set:
    """加载指定目录下已下载的图片文件名"""
    existing_files = set()
    for ext in ['*.png', '*.jpg', '*.jpeg', '*.gif']:
        existing_files.update(
            os.path.basename(f) for f in glob.glob(os.path.join(directory, ext))
        )
    return existing_files

async def process_cards():
    # 确保目录存在
    os.makedirs('images/brands', exist_ok=True)
    os.makedirs('images/cards', exist_ok=True)
    
    # 加载已存在的图片
    existing_brand_images = load_existing_images('images/brands')
    existing_card_images = load_existing_images('images/cards')
    print(f"Found {len(existing_brand_images)} existing brand images")
    print(f"Found {len(existing_card_images)} existing card images")
    
    # 加载信用卡数据
    try:
        with open('data/credit_cards_all.json', 'r', encoding='utf-8') as f:
            cards = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return
    
    # 收集所有需要下载的图片URL
    brand_urls = set()
    card_urls = set()
    
    for card in cards:
        # 添加卡片主图
        if 'image_url' in card:
            card_urls.add(card['image_url'])
        
        # 添加品牌图片
        if 'brands' in card:
            for brand in card['brands']:
                if 'image_url' in brand:
                    brand_urls.add(brand['image_url'])
    
    print(f"Found {len(brand_urls)} unique brand image URLs")
    print(f"Found {len(card_urls)} unique card image URLs")
    
    # 过滤掉已下载的图片
    new_brand_urls = []
    new_card_urls = []
    
    for url in brand_urls:
        filename = get_filename_from_url(url)
        if filename not in existing_brand_images:
            new_brand_urls.append(url)
    
    for url in card_urls:
        filename = get_filename_from_url(url)
        if filename not in existing_card_images:
            new_card_urls.append(url)
    
    print(f"Need to download {len(new_brand_urls)} new brand images")
    print(f"Need to download {len(new_card_urls)} new card images")
    
    # 下载新图片
    async with aiohttp.ClientSession() as session:
        # 下载品牌图片
        if new_brand_urls:
            print("\nDownloading brand images...")
            tasks = []
            for url in new_brand_urls:
                filename = get_filename_from_url(url)
                save_path = os.path.join('images/brands', filename)
                tasks.append(download_image(session, url, save_path))
            
            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)
            print(f"Brand images completed: {success_count} successful, {len(results) - success_count} failed")
        
        # 下载卡片图片
        if new_card_urls:
            print("\nDownloading card images...")
            tasks = []
            for url in new_card_urls:
                filename = get_filename_from_url(url)
                save_path = os.path.join('images/cards', filename)
                tasks.append(download_image(session, url, save_path))
            
            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)
            print(f"Card images completed: {success_count} successful, {len(results) - success_count} failed")
        
        if not new_brand_urls and not new_card_urls:
            print("No new images to download")

if __name__ == "__main__":
    asyncio.run(process_cards())
