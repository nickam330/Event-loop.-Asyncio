import asyncio
import requests
from itertools import batched
import aiohttp
from db import DbSession, SwapiPeople, close_orm, init_orm

MAX_REQUEST = 5

response = requests.get("https://www.swapi.tech/api/people?page=1&limit=10")
response = response.json()
MAX_PAGES = response['total_pages']


async def get_people(person_id: int, session: aiohttp.ClientSession):
    http_response = await session.get(f"https://www.swapi.tech/api/people/{person_id}/")
    json_data = await http_response.json()
    if json_data["message"] == "ok":
        json_data = json_data["result"]["properties"]

        json_data.pop('height')
        json_data.pop('films')
        json_data.pop('vehicles')
        json_data.pop('starships')
        json_data.pop('created')
        json_data.pop('edited')
        json_data.pop('url')
        return json_data
    return None


async def insert_people_batch(people_list: list[dict]):
    async with DbSession() as db_session:
        people_orm_obj = [SwapiPeople(json=item) for item in people_list]
        db_session.add_all(people_orm_obj)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for id_batch in batched(range(1, MAX_PAGES * 10), MAX_REQUEST):
            coros = [get_people(i, http_session) for i in id_batch]
            response = await asyncio.gather(*coros)
            insert_people_batch_coro = insert_people_batch(response)
            insert_people_batch_task = asyncio.create_task(insert_people_batch_coro)
    all_tasks = asyncio.all_tasks()
    main_task = asyncio.current_task()
    all_tasks.remove(main_task)
    for task in all_tasks:
        await task
    await close_orm()


asyncio.run(main())
