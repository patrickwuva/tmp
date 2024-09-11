import asyncio
import aiohttp
import json
import time
import pandas as pd
from add_offenders import insert_offenders, clean_offenders

def load_proxies(file_path):
    with open(file_path, "r") as f:
        proxies = f.read().splitlines()
    return proxies

def get_next_proxy(proxy_index, proxies_list):
    return proxies_list[proxy_index % len(proxies_list)]

async def get_offenders(session, zip_code, proxy, retries=3):
    search_url = "https://nsopw-api.ojp.gov/nsopw/v1/v1.0/search"
    search_headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "Referer": "https://www.nsopw.gov/",
        "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Brave\";v=\"127\", \"Chromium\";v=\"127\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Linux\"",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://www.nsopw.gov",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Sec-GPC": "1"
    }
    search_data = {
        "firstName": "",
        "lastName": "",
        "city": "",
        "county": "",
        "zips": [zip_code],
        "clientIp": ""
    }
    
    for attempt in range(retries):
        try:
            #proxy = 'http://spxwhjvleu:Bydk9qPurElL5_3q1v@us.smartproxy.com:10000'
            async with session.post(search_url, headers=search_headers, json=search_data, proxy=proxy) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'offenders' in data:
                        print(f"Retrieved offenders for ZIP {zip_code}")
                        return data['offenders']  # Returning offenders raw data
                    else:
                        print(f"No offenders data for ZIP {zip_code}")
                        return None
                elif response.status == 429:
                    print(f"Rate limited. Retrying ZIP {zip_code} after 3 seconds...")
                    await asyncio.sleep(3)
                else:
                    print(f"Failed request with status {response.status} for ZIP {zip_code}")
                    await asyncio.sleep(2)
        except Exception as e:
            print(f"Error occurred for ZIP {zip_code}: {e}. Retrying...")
            await asyncio.sleep(2)

    print(f"Failed to retrieve data for ZIP {zip_code} after {retries} attempts.")
    return None

# Asynchronous function to handle all zip codes
async def process_zips(zips, proxies_list, max_concurrent_requests=50):
    tasks = []
    proxy_index = 0

    # Set up a connector to manage concurrent sessions
    connector = aiohttp.TCPConnector(limit=max_concurrent_requests)
    
    # Use a single session for all requests
    async with aiohttp.ClientSession(connector=connector) as session:
        for zip_code in zips:
            proxy = get_next_proxy(proxy_index, proxies_list)
            proxy_index += 1  # Rotate to the next proxy

            task = asyncio.create_task(get_offenders(session, zip_code, proxy))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

    for zip_code, offenders in zip(zips, results):
        if offenders:
            insert_offenders(clean_offenders(offenders))
            print(f"ZIP {zip_code}: Retrieved {len(offenders)} offenders")
        else:
            print(f"ZIP {zip_code}: No offenders or failed retrieval")

# Main function
def main():
    # Load ZIP codes and proxies
    us_zips = pd.read_csv('zips.csv')
    zips = us_zips[us_zips['state'] == 'NY']['zip'].tolist()
    zips = [str(z).zfill(5) for z in zips]  # Ensure all zips are 5 digits long

    zips = zips[:500]
    proxies_list = load_proxies("endpoints.txt")

    asyncio.run(process_zips(zips, proxies_list))

if __name__ == '__main__':
    main()
