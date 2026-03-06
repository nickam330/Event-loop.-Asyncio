import asyncio
from itertools import batched
import aiohttp

from db import DbSession, SwapiPeople, close_orm, init_orm

MAX_REQUEST = 5


async def get_max_pages(session: aiohttp.ClientSession):
    http_response = await session.get("https://www.swapi.tech/api/people?page=1&limit=10")
    response = await http_response.json()
    return response["total_pages"]


async def get_people(person_id: int, session: aiohttp.ClientSession):
    http_response = await session.get(f"https://www.swapi.tech/api/people/{person_id}/")
    json_data = await http_response.json()
    if json_data["message"] == "ok":
        json_data = json_data["result"]
        print(json_data)
        return json_data
    return None


async def get_homeworld(session: aiohttp.ClientSession, http_homeworld: str):
    http_response = await session.get(http_homeworld)
    json_data = await http_response.json()
    return json_data["result"]["properties"]["name"]


async def insert_people_batch(people_list: list[dict]):
    async with DbSession() as db_session:
        for people in people_list:
            if people is None:
                continue
            async with aiohttp.ClientSession() as http_session:
                homeworld = await get_homeworld(http_session, people["properties"]["homeworld"])
            people_orm_obj = SwapiPeople(
                uid=people["uid"],
                birth_year=people["properties"]["birth_year"],
                eye_color=people["properties"]["eye_color"],
                gender=people["properties"]["gender"],
                hair_color=people["properties"]["hair_color"],
                homeworld=homeworld,
                mass=people["properties"]["mass"],
                name=people["properties"]["name"],
                skin_color=people["properties"]["skin_color"],
            )
            db_session.add(people_orm_obj)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        max_pages = await get_max_pages(http_session)
        for id_batch in batched(range(1, max_pages * 10 + 1), MAX_REQUEST):
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
